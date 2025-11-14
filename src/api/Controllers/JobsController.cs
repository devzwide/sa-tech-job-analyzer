using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class JobsController : ControllerBase
    {
        private readonly api.Data.JobDbContext _context;

        public JobsController(api.Data.JobDbContext context)
        {
            _context = context;
        }

        [HttpGet]
        public async Task<ActionResult<IEnumerable<api.Models.JobPosting>>> GetAllJobs()
        {
            var jobs = await _context.JobPostings.ToListAsync();
            
            return Ok(jobs);
        }

        [HttpGet("{province}")]
        public async Task<ActionResult<IEnumerable<api.Models.JobPosting>>> GetJobsByProvince(string province)
        {
            var jobs = await _context.JobPostings
                .Where(job =>job.Province.ToLower() == province.ToLower())
                .ToListAsync();

            if (jobs == null || jobs.Count == 0)
            {
                return NotFound($"No job postings found for province: {province}");
            }

            return Ok(jobs);
        }
    }
}