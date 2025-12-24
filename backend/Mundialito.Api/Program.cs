using Microsoft.EntityFrameworkCore;
using Mundialito.Api.Data;

var builder = WebApplication.CreateBuilder(args);

var repoRoot = Directory.GetParent(builder.Environment.ContentRootPath)!.FullName;
Console.WriteLine(repoRoot);
var externalConfigPath = Path.Combine(repoRoot, "../config.json");

builder.Configuration.AddJsonFile(externalConfigPath, optional: false, reloadOnChange: true);

var server   = builder.Configuration["server"];
var database = builder.Configuration["database"];
var username = builder.Configuration["username"];
var password = builder.Configuration["password"];

string connectionString;

if (string.IsNullOrWhiteSpace(username))
{
    connectionString =
        $"Server={server};Database={database};Trusted_Connection=True;TrustServerCertificate=True";
}
else
{
    connectionString =
        $"Server={server};Database={database};User Id={username};Password={password};TrustServerCertificate=True";
}

builder.Services.AddOpenApi();

builder.Services.AddControllers();

builder.Services.AddDbContext<MundialitoDbContext>(options =>
    options.UseSqlServer(connectionString));

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.MapControllers();

app.Run();
