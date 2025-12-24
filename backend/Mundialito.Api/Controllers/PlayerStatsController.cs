using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Mundialito.Api.Data;
using Mundialito.Api.Models;

namespace Mundialito.Api.Controllers;

[ApiController]
[Route("api/tournaments/{tournamentId}/players")]
public class PlayerStatsController : ControllerBase
{
    private readonly MundialitoDbContext _db;

    public PlayerStatsController(MundialitoDbContext db)
    {
        _db = db;
    }

    // GET /api/tournaments/4/players
    [HttpGet]
    public async Task<ActionResult<IEnumerable<PlayerTournamentStatDto>>> GetPlayerStats(int tournamentId)
    {
        var query =
            from pts in _db.PlayerTournamentStats
            join p in _db.Players on pts.PlayerId equals p.PlayerId
            join t in _db.Teams on pts.TeamId equals t.TeamId
            where pts.MundialitoId == tournamentId
            orderby pts.Goals descending, pts.Assists descending
            select new PlayerTournamentStatDto
            {
                PlayerId = pts.PlayerId,
                PlayerName = p.Name,
                TeamId = pts.TeamId,
                TeamName = t.TeamName,
                TeamAbbr = t.TeamAbbr,
                MundialitoId = pts.MundialitoId,
                GamesPlayed = pts.GamesPlayed,
                GamesWon = pts.GamesWon,
                GamesDrawn = pts.GamesDrawn,
                GamesLost = pts.GamesLost,
                Goals = pts.Goals,
                Assists = pts.Assists,
                CleanSheets = pts.CleanSheets,
                GoalsConceded = pts.GoalsConceded
            };

        var stats = await query.ToListAsync();

        return Ok(stats);
    }
}
