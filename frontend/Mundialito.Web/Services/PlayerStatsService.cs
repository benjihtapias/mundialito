using System.Net.Http;
using System.Net.Http.Json;
using Mundialito.Web.Models;

namespace Mundialito.Web.Services;

public class PlayerStatsService
{
    private readonly HttpClient _httpClient;

    public PlayerStatsService(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<List<PlayerTournamentStatsDto>> GetPlayerStatsAsync(int mundialitoId)
    {
        var url = $"api/tournaments/{mundialitoId}/players";

        var result = await _httpClient.GetFromJsonAsync<List<PlayerTournamentStatsDto>>(url);

        return result ?? new List<PlayerTournamentStatsDto>();
    }
}
