using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class PipelineController : ControllerBase
    {
        private readonly ILogger<PipelineController> _logger;
        private readonly IConfiguration _config;

        private static readonly SemaphoreSlim _pipelineLock = new SemaphoreSlim(1, 1);

        public PipelineController(ILogger<PipelineController> logger, IConfiguration config)
        {
            _logger = logger;
            _config = config;
        }

        [HttpPost("run")]
        public IActionResult RunPipeline()
        {
            _logger.LogInformation("Received request to run ETL pipeline.");

            if (!_pipelineLock.Wait(0))
            {
                _logger.LogWarning("Pipeline is already running. Another run cannot be started.");
                
                return Conflict("Pipeline is already running. Please try again later.");
            }

            _logger.LogInformation("Acquired pipeline lock. Starting background process.");

            Task.Run(() =>
            {
                try
                {
                    var pythonExecutable = _config["PipelineSettings:PythonExecutable"];
                    var scriptPath = _config["PipelineSettings:ScriptPath"];
                    var workingDirectory = _config["PipelineSettings:WorkingDirectory"];

                    if (string.IsNullOrEmpty(pythonExecutable) || string.IsNullOrEmpty(scriptPath) || string.IsNullOrEmpty(workingDirectory))
                    {
                        _logger.LogError("Pipeline settings are missing from appsettings.json. Cannot start pipeline.");
                        return; 
                    }
                    
                    var processStartInfo = new ProcessStartInfo
                    {
                        FileName = Path.GetFullPath(Path.Combine(workingDirectory, pythonExecutable)),

                        Arguments = Path.GetFullPath(Path.Combine(workingDirectory, scriptPath)),
                        
                        WorkingDirectory = Path.GetFullPath(workingDirectory),
                        
                        RedirectStandardOutput = true,
                        RedirectStandardError = true,
                        UseShellExecute = false,
                        CreateNoWindow = true
                    };

                    using (var process = new Process { StartInfo = processStartInfo })
                    {
                        process.OutputDataReceived += (sender, args) => {
                            if (args.Data != null) _logger.LogInformation($"[Pipeline Output]: {args.Data}");
                        };
                        process.ErrorDataReceived += (sender, args) => {
                            if (args.Data != null) _logger.LogError($"[Pipeline Error]: {args.Data}");
                        };

                        _logger.LogInformation($"Starting process: {process.StartInfo.FileName} {process.StartInfo.Arguments}");
                        
                        process.Start();
                        process.BeginOutputReadLine();
                        process.BeginErrorReadLine();

                        process.WaitForExit(); 
                        _logger.LogInformation($"Pipeline process finished with exit code {process.ExitCode}.");
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "An unhandled exception occurred during pipeline execution.");
                }
                finally
                {
                    _pipelineLock.Release();
                    _logger.LogInformation("Pipeline lock released.");
                }
            });

            return Accepted(new { message = "Pipeline run initiated." });
        }
    }
}