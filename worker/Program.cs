using System;
using System.Collections.Generic;
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

            // === ПАРСИНГ POSTGRESQL URL В СТАНДАРТНУЮ СТРОКУ ПОДКЛЮЧЕНИЯ ===
            var pgUri = new Uri(pgUrl);
            var userInfo = pgUri.UserInfo.Split(':');
            string user = userInfo[0];
            string password = userInfo.Length > 1 ? userInfo[1] : "";
            string host = pgUri.Host;
            int port = pgUri.Port;
            string database = pgUri.AbsolutePath.TrimStart('/');

            string pgConnString = $"Host={host};Port={port};Database={database};Username={user};Password={password};";

            // === НАСТРОЙКА REDIS С ПОВТОРНЫМИ ПОПЫТКАМИ ===
            var redisOptions = ConfigurationOptions.Parse(redisUrl);
            redisOptions.AbortOnConnectFail = false;   // не падать при неудаче
            redisOptions.ConnectRetry = 5;
            redisOptions.ConnectTimeout = 5000;
            redisOptions.SyncTimeout = 5000;
            redisOptions.KeepAlive = 60;

            var redis = ConnectionMultiplexer.Connect(redisOptions);
            var db = redis.GetDatabase();

            // === ПОДКЛЮЧЕНИЕ К POSTGRESQL ===
            await using var pgConn = new NpgsqlConnection(pgConnString);
            await pgConn.OpenAsync();

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
    }
}