using System.Net.Http;
using System.Net.Http.Json;
using Mundialito.Web.Models;

namespace Mundialito.Web.Services;

public class TournamentService
{
    private readonly HttpClient _httpClient;

    public TournamentService(HttpClient httpClient)
    {
        _httpClient = httpClient;
    }

    public async Task<List<MundialitosDto>> GetTournamentsAsync()
    {
        var result = await _httpClient.GetFromJsonAsync<List<MundialitosDto>>("api/tournaments");
        return result ?? new List<MundialitosDto>();
    }
}
