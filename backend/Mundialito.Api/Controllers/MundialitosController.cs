using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using Mundialito.Api.Data;
using Mundialito.Api.Models;

namespace Mundialito.Api.Controllers;

[ApiController]
[Route("api/tournaments")]
public class MundialitosController : ControllerBase
{
    private readonly MundialitoDbContext _db;

    public MundialitosController(MundialitoDbContext db)
    {
        _db = db;
    }
    
    [HttpGet]
    public async Task<ActionResult<IEnumerable<Mundialitos>>> GetAll()
    {
        var items = await _db.Mundialitos
            .OrderBy(m => m.MundialitoId)
            .ToListAsync();

        return Ok(items);
    }
}
