using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace api.Models
{
    [Table("JobPostings")] 
    public class JobPosting
    {
        [Key] 
        public string Id { get; set; } = string.Empty;

        public string Title { get; set; } = string.Empty;
        public string Company { get; set; } = string.Empty;
        public string Location { get; set; } = string.Empty;
        public string Province { get; set; } = string.Empty;

        [Column("min_salary_pa")] 
        public long? MinSalaryPa { get; set; } 

        [Column("max_salary_pa")]
        public long? MaxSalaryPa { get; set; } 

        public string Skills { get; set; } = string.Empty;
    }
}
