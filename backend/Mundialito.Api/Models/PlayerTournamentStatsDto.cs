namespace Mundialito.Api.Models;

public class PlayerTournamentStatDto
{
    public int PlayerId { get; set; }
    public string PlayerName { get; set; } = string.Empty;

    public int TeamId { get; set; }
    public string TeamName { get; set; } = string.Empty;
    public string TeamAbbr { get; set; } = string.Empty;

    public int MundialitoId { get; set; }

    public int GamesPlayed { get; set; }
    public int GamesWon { get; set; }
    public int GamesDrawn { get; set; }
    public int GamesLost { get; set; }

    public int Goals { get; set; }
    public int Assists { get; set; }

    public int? CleanSheets { get; set; }
    public int? GoalsConceded { get; set; }
}
