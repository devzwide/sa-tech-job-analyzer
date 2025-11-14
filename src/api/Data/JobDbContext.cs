using System;
using Microsoft.EntityFrameworkCore;

namespace api.Data
{
    public class JobDbContext : DbContext
    {
        public JobDbContext(DbContextOptions<JobDbContext> options) : base(options)
        {
        }

        public DbSet<api.Models.JobPosting> JobPostings { get; set; }
    }
}