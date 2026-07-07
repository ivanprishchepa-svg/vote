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
            // Читаем переменные окружения
            string redisUrl = Environment.GetEnvironmentVariable("REDIS_URL") ?? "redis://redis:6379";
            string pgConnString = Environment.GetEnvironmentVariable("DATABASE_URL") 
                ?? "Host=postgres;Database=votesdb;Username=postgres;Password=postgres";

            // Настройка Redis с отложенным повторным подключением
            var redisOptions = ConfigurationOptions.Parse(redisUrl);
            redisOptions.AbortOnConnectFail = false;   // не падать при неудаче
            redisOptions.ConnectRetry = 5;             // повторить 5 раз
            redisOptions.ConnectTimeout = 5000;        // таймаут подключения 5 секунд
            redisOptions.SyncTimeout = 5000;           // таймаут синхронных операций
            redisOptions.KeepAlive = 60;               // keep-alive каждые 60 секунд

            var redis = ConnectionMultiplexer.Connect(redisOptions);
            var db = redis.GetDatabase();

            // Подключение к PostgreSQL
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

            // Бесконечный цикл обработки голосов
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
                    // Не прерываем цикл, ждём и продолжаем
                    await Task.Delay(1000);
                }
            }
        }
    }
}