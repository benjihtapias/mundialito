using Microsoft.EntityFrameworkCore;
using Mundialito.Api.Models;

namespace Mundialito.Api.Data;

public class MundialitoDbContext : DbContext
{
    public MundialitoDbContext(DbContextOptions<MundialitoDbContext> options)
        : base(options)
    {
    }

    public DbSet<PlayerTournamentStats> PlayerTournamentStats { get; set; }
    public DbSet<Player> Players { get; set; }
    public DbSet<Team> Teams { get; set; }
    public DbSet<Mundialitos> Mundialitos { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<PlayerTournamentStats>(entity =>
        {
            entity.ToTable("PlayerTournamentStats");
            entity.HasKey(e => new { e.PlayerId, e.MundialitoId });
        });

        modelBuilder.Entity<Player>(entity =>
        {
            entity.ToTable("Players");
            entity.HasKey(e => e.PlayerId);
        });

        modelBuilder.Entity<Team>(entity =>
        {
            entity.ToTable("Teams");
            entity.HasKey(e => e.TeamId);
        });

        modelBuilder.Entity<Mundialitos>(entity =>
        {
            entity.ToTable("Mundialitos");
            entity.HasKey(e => e.MundialitoId);
        });

        base.OnModelCreating(modelBuilder);
    }
}
