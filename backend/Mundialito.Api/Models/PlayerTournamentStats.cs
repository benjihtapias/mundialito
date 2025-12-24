namespace Mundialito.Api.Models;

public class PlayerTournamentStats
{
    public int PlayerId { get; set; }
    public int MundialitoId { get; set; }
    public int TeamId { get; set; }

    public int GamesPlayed { get; set; }
    public int GamesWon { get; set; }
    public int GamesDrawn { get; set; }
    public int GamesLost { get; set; }

    public int Goals { get; set; }
    public int Assists { get; set; }

    public int? CleanSheets { get; set; }     // nullable since old data might not have it
    public int? GoalsConceded { get; set; }   // nullable as well
}
