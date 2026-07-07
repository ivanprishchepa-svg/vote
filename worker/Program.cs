using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using StackExchange.Redis;

namespace VoteWorker
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Чтение переменных окружения
            string redisUrl = Environment.GetEnvironmentVariable("REDIS_URL") ?? "redis://redis:6379";
            string pgUrl = Environment.GetEnvironmentVariable("DATABASE_URL") 
                ?? "postgresql://postgres:postgres@postgres:5432/votesdb";

            Console.WriteLine($"Raw REDIS_URL: {redisUrl}");

            // === ПАРСИНГ REDIS URL ===
            var (redisHost, redisPort, redisPassword) = ParseRedisUrl(redisUrl);
            Console.WriteLine($"Parsed Redis: host={redisHost}, port={redisPort}, password={redisPassword != ""}");

            // Настройка Redis с ручным указанием параметров
            var redisOptions = new ConfigurationOptions
            {
                AbortOnConnectFail = false,   // не падать при неудаче
                ConnectRetry = 5,
                ConnectTimeout = 5000,
                SyncTimeout = 5000,
                KeepAlive = 60,
                Password = redisPassword,
                DefaultDatabase = 0,
            };
            redisOptions.EndPoints.Add(redisHost, redisPort);

            // Асинхронное подключение с ожиданием готовности
            var redis = await ConnectionMultiplexer.ConnectAsync(redisOptions);
            while (!redis.IsConnected)
            {
                Console.WriteLine("Ожидание подключения к Redis...");
                await Task.Delay(500);
            }
            Console.WriteLine("Подключение к Redis установлено.");
            var db = redis.GetDatabase();

            // === ПАРСИНГ POSTGRESQL URL ===
            var pgUri = new Uri(pgUrl);
            var userInfo = pgUri.UserInfo.Split(':');
            string user = userInfo[0];
            string password = userInfo.Length > 1 ? userInfo[1] : "";
            string host = pgUri.Host;
            int port = pgUri.Port;
            string database = pgUri.AbsolutePath.TrimStart('/');

            string pgConnString = $"Host={host};Port={port};Database={database};Username={user};Password={password};";
            Console.WriteLine($"PostgreSQL connection string: {pgConnString}");

            // === ПОДКЛЮЧЕНИЕ К POSTGRESQL ===
            await using var pgConn = new NpgsqlConnection(pgConnString);
            await pgConn.OpenAsync();
            Console.WriteLine("Подключение к PostgreSQL установлено.");

            // Создаём таблицу, если её нет
            await using var cmd = new NpgsqlCommand(
                @"CREATE TABLE IF NOT EXISTS votes (
                    id SERIAL PRIMARY KEY,
                    option VARCHAR(50) UNIQUE NOT NULL,
                    count INTEGER DEFAULT 0
                );", pgConn);
            await cmd.ExecuteNonQueryAsync();

            Console.WriteLine("Worker запущен. Ожидание голосов...");

            // === БЕСКОНЕЧНЫЙ ЦИКЛ ОБРАБОТКИ ===
            while (true)
            {
                try
                {
                    var vote = await db.ListRightPopAsync("votes");
                    if (!vote.HasValue)
                    {
                        await Task.Delay(100);
                        continue;
                    }

                    string option = vote.ToString();
                    Console.WriteLine($"Получен голос за '{option}'");

                    // Upsert в PostgreSQL
                    await using var upsertCmd = new NpgsqlCommand(
                        @"INSERT INTO votes (option, count) 
                          VALUES (@option, 1) 
                          ON CONFLICT (option) 
                          DO UPDATE SET count = votes.count + 1;", pgConn);
                    upsertCmd.Parameters.AddWithValue("option", option);
                    await upsertCmd.ExecuteNonQueryAsync();

                    // Получаем актуальные счётчики
                    await using var selectCmd = new NpgsqlCommand(
                        "SELECT option, count FROM votes;", pgConn);
                    await using var reader = await selectCmd.ExecuteReaderAsync();
                    var results = new Dictionary<string, int>();
                    while (await reader.ReadAsync())
                        results[reader.GetString(0)] = reader.GetInt32(1);
                    await reader.CloseAsync();

                    string json = JsonSerializer.Serialize(results);
                    await db.PublishAsync(RedisChannel.Literal("results"), json);
                    await db.StringSetAsync("latest_results", json);

                    Console.WriteLine($"Обработан голос за '{option}'. Результат: {json}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Ошибка при обработке: {ex.Message}");
                    await Task.Delay(1000);
                }
            }
        }

        /// <summary>
        /// Парсит Redis URL, извлекая хост, порт и пароль.
        /// Обрабатывает случаи с дублирующимся портом.
        /// </summary>
        static (string host, int port, string password) ParseRedisUrl(string url)
        {
            // Удаляем схему redis:// (если есть)
            string withoutScheme = url.StartsWith("redis://") ? url.Substring(8) : url;
            
            // Отделяем часть с логином/паролем до @
            string userInfo = "";
            string hostPort = withoutScheme;
            int atIndex = withoutScheme.IndexOf('@');
            if (atIndex != -1)
            {
                userInfo = withoutScheme.Substring(0, atIndex);
                hostPort = withoutScheme.Substring(atIndex + 1);
            }

            // Извлекаем пароль из userInfo (формат default:password)
            string password = "";
            if (!string.IsNullOrEmpty(userInfo))
            {
                var parts = userInfo.Split(':');
                password = parts.Length > 1 ? parts[1] : parts[0];
            }

            // Определяем хост и порт
            string host = hostPort;
            int port = 6379; // порт по умолчанию

            // Ищем последнее двоеточие для отделения порта
            int lastColon = hostPort.LastIndexOf(':');
            if (lastColon != -1)
            {
                // Проверяем, не является ли это IPv6 (там много двоеточий)
                if (hostPort.Count(c => c == ':') > 1)
                {
                    // Для IPv6 хост обычно в квадратных скобках, но мы обработаем просто: оставляем как есть
                    // В нашем случае это обычный домен, так что просто берём последнюю часть
                }
                // Пробуем распарсить порт
                string portStr = hostPort.Substring(lastColon + 1);
                if (int.TryParse(portStr, out int parsedPort))
                {
                    // Если успешно, то это порт
                    port = parsedPort;
                    host = hostPort.Substring(0, lastColon);
                }
                // Если не удалось распарсить, то, возможно, это часть хоста (например, в IPv6)
            }

            return (host, port, password);
        }
    }
}