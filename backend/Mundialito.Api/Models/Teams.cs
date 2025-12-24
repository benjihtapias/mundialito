namespace Mundialito.Api.Models;

public class Team
{
    public int TeamId { get; set; }
    public int MundialitoId { get; set; }
    public string TeamName { get; set; } = string.Empty;
    public string TeamAbbr { get; set; } = string.Empty;
}