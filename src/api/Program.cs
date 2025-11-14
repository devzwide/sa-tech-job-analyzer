using Microsoft.EntityFrameworkCore;
using api.Data;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();

// Use AddOpenApi(), which is correct for your 'Microsoft.AspNetCore.OpenApi' package
builder.Services.AddOpenApi();

builder.Services.AddDbContext<api.Data.JobDbContext>(options =>
    options.UseSqlServer(builder.Configuration.GetConnectionString("DefaultConnection")));

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    // Use MapOpenApi(), which is the correct corresponding method
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();