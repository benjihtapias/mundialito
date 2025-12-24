namespace Mundialito.Api.Models;

public class Mundialitos
{
    public int MundialitoId { get; set; }
    public string Name { get; set; } = string.Empty;
    public DateOnly Date { get; set; }
}