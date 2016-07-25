using System;
using System.Web.Mail;

namespace Quantum_QFOR
{
    public class clsMail
    {
        public string[] args = new string[7];
        public string strDelimiter;
        public string strAttachmentList;
        public void SendMail(string strMailServer)
        {
            try
            {
                try
                {
                    MailMessage Message = new MailMessage();
                    Message.To = args[0];
                    Message.From = args[1];
                    Message.Subject = args[2];
                    Message.Body = args[3];
                    Message.Cc = args[4].ToString();
                    Message.BodyFormat = (MailFormat)args.GetValue(5);
                    try
                    {
                        SmtpMail.SmtpServer = strMailServer;
                        SmtpMail.Send(Message);
                    }
                    catch (System.Web.HttpException ehttp)
                    {
                        Console.WriteLine("0", ehttp.Message);
                        Console.WriteLine("Here is the full error message");
                        Console.Write("0", ehttp.ToString());
                        throw ehttp;
                    }
                }
                catch (IndexOutOfRangeException e)
                {
                }
            }
            catch (System.Exception e)
            {
                Console.WriteLine("Unknown Exception occurred 0", e.Message);
                Console.WriteLine("Here is the Full Error Message");
                Console.WriteLine("0", e.ToString());
                throw e;
            }
        }


    }

}
