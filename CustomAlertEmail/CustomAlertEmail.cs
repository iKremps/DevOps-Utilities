using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using System.Net.Mail;
using System.Net;
using CommonUtilityCode;

namespace CustomAlertEmail
{
    public class CustomAlertEmail
    {

        private static readonly string SMTP = Environment.GetEnvironmentVariable("SMTP");
        //private static readonly dynamic Port = Environment.GetEnvironmentVariable("Port");
        private static readonly string Username = Environment.GetEnvironmentVariable("testUsername");
        private static readonly string Password = Environment.GetEnvironmentVariable("Password");
        private static readonly dynamic PortNumber = Environment.GetEnvironmentVariable("PortNumber");
        private static readonly dynamic EnableSsl = Environment.GetEnvironmentVariable("EnableSsl");

        private static dynamic jsonObj;
        private static string body; //body of the email. will be a template that can be modified



        public void BaseFunction()
        {
            try
            {
                var smtpClient = new SmtpClient(SMTP) //host of server
                {
                    //UseDefaultCredentials = false, //THIS IS TEMP. COMMENT OUT WHEN DONE
                    //TargetName = $"STARTTLS/{SMTP}",
                    Port = int.Parse(PortNumber), //config
                    Credentials = new NetworkCredential(Username, Password),
                    EnableSsl = bool.Parse(EnableSsl), //config
                };

                //smtpClient.Send("email", "recipient", "subject", "body");

                var mailMessage = new MailMessage
                {
                    From = new MailAddress("EDIService@cannonsecurityproducts.com"), //email of sender noreply@partnerlinq.net
                    Subject = "TEST", 
                    //Body = body,
                    Body = $"<h1>This is a test email from host: {SMTP}</h1>",
                    IsBodyHtml = true,
                };

                mailMessage.To.Add("Ian.krempa@visionetsystems.com"); //email of recipient
               

                smtpClient.Send(mailMessage);
            }
            catch (Exception ex)
            {
                ErrorHandling.throwErrorNormal(ex);
            }

        }





        [FunctionName("CustomAlertEmail")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {

            var request = await req.ReadAsStringAsync();
            jsonObj = JsonConvert.DeserializeObject<dynamic>(request);

            BaseFunction();

            return new OkResult();
        }
    }
}
