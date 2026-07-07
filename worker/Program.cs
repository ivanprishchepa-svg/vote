using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using StackExchange.Redis;

// Читаем строки подключения из переменных окружения
string redisUrl = Environment.GetEnvironmentVariable("REDIS_URL") ?? "redis://redis:6379";
string pgConnString = Environment.GetEnvironmentVariable("DATABASE_URL") ?? "Host=postgres;Database=votesdb;Username=postgres;Password=postgres";

// Подключаемся к Redis
var redis = ConnectionMultiplexer.Connect(redisUrl);
var db = redis.GetDatabase();

// Подключаемся к PostgreSQL
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

while (true)
{
    // Блокирующее чтение из очереди Redis (список 'votes')
    var vote = await db.ListRightPopAsync("votes");
    if (!vote.HasValue)
    {
        await Task.Delay(100);
        continue;
    }

    string option = vote.ToString();

    // Атомарное увеличение счётчика (Upsert)
    await using var upsertCmd = new NpgsqlCommand(
        @"INSERT INTO votes (option, count) 
          VALUES (@option, 1) 
          ON CONFLICT (option) 
          DO UPDATE SET count = votes.count + 1;", pgConn);
    upsertCmd.Parameters.AddWithValue("option", option);
    await upsertCmd.ExecuteNonQueryAsync();

    // Читаем актуальные счётчики
    await using var selectCmd = new NpgsqlCommand(
        "SELECT option, count FROM votes;", pgConn);
    await using var reader = await selectCmd.ExecuteReaderAsync();
    var results = new Dictionary<string, int>();
    while (await reader.ReadAsync())
        results[reader.GetString(0)] = reader.GetInt32(1);
    await reader.CloseAsync();

    // Публикуем обновление в Redis-канал 'results'
    string json = JsonSerializer.Serialize(results);
    await db.PublishAsync(RedisChannel.Literal("results"), json);
    // Сохраняем последние результаты для новых клиентов
    await db.StringSetAsync("latest_results", json);
}
