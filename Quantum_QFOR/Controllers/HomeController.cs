using System;
using System.Web.Mvc;

namespace Quantum_QFOR.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Index()
        {
            AdministratorController admnc = new AdministratorController();
            //var items = admnc.GetMenuItems();
            //ViewBag.Title = "Home Page";
            //JsonResult var = Json(items, JsonRequestBehavior.AllowGet);
            return View();
        }

        public ActionResult Navigate(Int64 Pageid)
        {
            AdministratorController admnc = new AdministratorController();
            var items = admnc.GetMenuItems(Pageid);
            ViewBag.Title = "Home Page";
            return View(items);
        }
    }
}