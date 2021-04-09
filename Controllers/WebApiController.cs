using bz.iteam.crm.data;
using Microsoft.AspNet.Identity;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Net;
using System.Web.Mvc;

namespace bz.iteam.crm.web.Controllers
{
    public partial class WebApiController : Controller
    {
        protected readonly Repository db = new Repository();
        protected readonly stor.Storage storage = new stor.Storage();
        protected readonly docs.Documents documents = new docs.Documents();

        [HttpPost]
        public ActionResult LogoutOperator(string number)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                var opers = db.OperatorStateHistories.Where(e => e.LineNumber == number);

                if (opers.Count() > 0)
                {
                    var oper = opers.OrderByDescending(e => e.CreationDate).FirstOrDefault();
                    var shift = db.CallProjectShifts.Where(e => e.CallProject_Id == oper.CallProject_Id).FirstOrDefault();
                    db.OperatorStateHistories.Add(new OperatorStateHistory
                    {
                        Operator_Id = oper.Operator_Id,
                        State = OperatorState.Offline,
                        CreationDate = DateTime.Now,
                        LineNumber = number,
                        Reason = OperatorStateReason.OperatorLogout,
                        CallProject_Id = oper.CallProject_Id,
                        CallProjectShift_Id = shift.Id
                    });
                    db.SaveChanges();
                    return Content("Ok");
                }
                else
                {
                    return Content("NOT OPERATOR");
                }
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpPost]
        public ActionResult LoginOperator(string AuthCode, string number, string shift)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                var oper = db.Operators.Where(e => e.AuthCode == AuthCode && e.Status == HREmployeeStatus.Hired).FirstOrDefault();
                if (oper != null)
                {
                    var projectOp = db.CallProjectOperators.Where(e => e.Operator_Id == oper.Id).FirstOrDefault();
                    if (projectOp == null)
                    {
                        return Content("NO PROJECT");
                    }

                    var shifts = db.CallProjectShifts.Where(e => e.CallProject_Id == projectOp.CallProject_Id).ToList();

                    if (shifts.Count() > 0)
                    {
                        if (shift != "" && shift != null)
                        {
                            foreach (var s in shifts)
                            {
                                if (s.Index == Convert.ToInt32(shift))
                                {
                                    db.OperatorStateHistories.Add(new OperatorStateHistory
                                    {
                                        Operator_Id = oper.Id,
                                        State = OperatorState.Online,
                                        CreationDate = DateTime.Now,
                                        LineNumber = number,
                                        Reason = OperatorStateReason.OperatorLogin,
                                        CallProject_Id = projectOp.CallProject_Id,
                                        CallProjectShift = s
                                    });
                                    db.SaveChanges();
                                    return Content("Ok");
                                }
                            }
                            return Content("NO SHIFT");
                        }

                        if (shifts.Count() == 1)
                        {
                            db.OperatorStateHistories.Add(new OperatorStateHistory
                            {
                                Operator_Id = oper.Id,
                                State = OperatorState.Online,
                                CreationDate = DateTime.Now,
                                LineNumber = number,
                                Reason = OperatorStateReason.OperatorLogin,
                                CallProject_Id = projectOp.CallProject_Id,
                                CallProjectShift_Id = shifts.First().Id
                            });
                            db.SaveChanges();
                            return Content("Ok");
                        }
                        else
                        {
                            return Content("MORE SHIFT");
                        }
                    }
                    else
                    {
                        return Content("NO SHIFT");
                    }
                }
                else
                {
                    return Content("NO AUTH");
                }
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpPost]
        public ActionResult LoginOperatorManual(string AuthCode, string number, string shift)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                var oper = db.Operators.Where(e => e.AuthCode == AuthCode && e.Status == HREmployeeStatus.Hired).FirstOrDefault();
                if (oper != null)
                {
                    var projectOp = db.CallProjectOperators.Where(e => e.Operator_Id == oper.Id).FirstOrDefault();
                    if (projectOp == null)
                    {
                        return Content("NO PROJECT");
                    }

                    var shifts = db.CallProjectShifts.Where(e => e.CallProject_Id == projectOp.CallProject_Id).ToList();
                    if (shifts.Count() > 0)
                    {
                        if (shift != "")
                        {
                            foreach (var s in shifts)
                            {
                                if (s.Index == Convert.ToInt32(shift))
                                {
                                    db.OperatorStateHistories.Add(new OperatorStateHistory
                                    {
                                        Operator_Id = oper.Id,
                                        State = OperatorState.Manual,
                                        CreationDate = DateTime.Now,
                                        LineNumber = number,
                                        Reason = OperatorStateReason.OperatorLogin,
                                        CallProject_Id = projectOp.CallProject_Id,
                                        CallProjectShift_Id = s.Index
                                    });
                                    db.SaveChanges();
                                    return Content("Ok");
                                }
                            }
                            return Content("NO SHIFT");
                        }

                        if (shifts.Count() == 1)
                        {
                            db.OperatorStateHistories.Add(new OperatorStateHistory
                            {
                                Operator_Id = oper.Id,
                                State = OperatorState.Manual,
                                CreationDate = DateTime.Now,
                                LineNumber = number,
                                Reason = OperatorStateReason.OperatorLogin,
                                CallProject_Id = projectOp.CallProject_Id,
                                CallProjectShift_Id = shifts.First().Id
                            });
                            db.SaveChanges();
                            return Content("Ok");
                        }
                        else
                        {
                            return Content("MORE SHIFT");
                        }
                    }
                    else
                    {
                        return Content("NO SHIFT");
                    }
                }
                else
                {
                    return Content("NO AUTH");
                }
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpPost]
        public ActionResult WriteDTMF(string uuid, string number, string operatorFS, int dTMf)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                if (db.CallProjectTestNumbers.Any(e => e.Phone == number)) return Content("OK");  // тестовый дозвон

                var fsOperatorSession = db.OperatorCalls.Where(e => new Guid(uuid) == e.Uuid).FirstOrDefault();
                long? projectId = null;
                OperatorCall cc = null;
                long? operator_Id = null;
                IEnumerable<OperatorStateHistory> opers;

                if (fsOperatorSession.Client_Id != null)
                {
                    return Content("CLIENT OK!");
                }

                if (fsOperatorSession != null)
                {
                    //if (fsOperatorSession.DtmfQuery == null || (int)fsOperatorSession.DtmfQuery != dTMf)
                    if (fsOperatorSession.DtmfQuery == null || !((int)fsOperatorSession.DtmfQuery == dTMf))
                    {
                        fsOperatorSession.DtmfQuery = (CallDtmfQuery)dTMf;
                        db.SaveChanges();
                        return Content("SAVE ONE DTMF");
                    }

                    projectId = fsOperatorSession.CallProject_Id;
                    number = fsOperatorSession.CallBaseNumber;

                    //cc = db.OperatorCalls.Where(e => e.Operator_Id == fsOperatorSession.Operator_Id && e.CallBaseNumber == number).OrderByDescending(e => e.TimeStart).FirstOrDefault();
                    cc = fsOperatorSession;
                    operator_Id = fsOperatorSession.Operator_Id;

                    opers = db.OperatorStateHistories.Where(e => e.LineNumber == number);
                }
                else
                {
                    opers = db.OperatorStateHistories.Where(e => e.LineNumber == number);
                    if (opers.Count() > 0)
                    {
                        var oper = opers.OrderByDescending(e => e.CreationDate).FirstOrDefault();

                        cc = db.OperatorCalls.Where(e => e.Operator_Id == oper.Operator_Id && e.CallBaseNumber == number).OrderByDescending(e => e.TimeStart).FirstOrDefault();

                        projectId = cc.CallProject_Id;
                        operator_Id = cc.Operator_Id;
                    }
                }

                //if (opers.Count() > 0)
                //{
                //    var oper = opers.OrderByDescending(e => e.CreationDate).FirstOrDefault();
                //    if (oper.State != OperatorState.Online && oper.State != OperatorState.Manual)
                //    {
                //        return Content("OPERATOR STATE NOT ONLINE OR MANUAL");
                //    }
                //}
                //else
                //{
                //    return Content("NOT OPERATOR");
                //}
                //var projectId = cc.CallProject_Id;

                if (dTMf != 0)
                {
                    db.Database.ExecuteSqlCommand($"update { db.Database.Connection.Database}.dbo.CallBaseNumbers_{projectId} set Descr = '{dTMf}', State = 'C' where Phone = {number}");
                }

                if (cc != null)
                {
                    cc.DtmfQuery = (CallDtmfQuery)dTMf;
                    db.SaveChanges();
                }

                if (dTMf == 9)
                {
                    db.Database.ExecuteSqlCommand($"insert	{db.DbName()}.dbo.StopListNumbers (Phone, CreationDate, CreationMode, CreatorName) " +
    $"select	case when left({number}, 1) = 0 then right({number}, len(nullif({number}, '')) - 1) else {number} end, getDate(), 2, o.Name + ' (' + cc.Name + ')' " +
    $"from {db.DbName()}.dbo.Operators o " +
    $"join {db.DbName()}.dbo.CallCenters cc" +
    $"on o.CallCenter_Id = cc.Id " +
    $"where o.Id = {operator_Id}");
                }
                //  Supervisor supervisor = null;
                if (dTMf == 0)
                {
                    db.Database.ExecuteSqlCommand($"update {db.DbName()}.dbo.CallbaseNumbers_{projectId} " +
    $"set Confirmed = 1 " +
    //$"where Id = {callBaseNumbers} " +
    $"where Phone = {number}" +

    $"update {db.DbName()}.dbo.CallBases " +
    $"set     LastAssignDate = getDate() " +
    $"where Id = ( " +
    $"            select  CallBase_Id " +
    $"            from {db.DbName()}.dbo.CallbaseNumbers_{projectId} " +
    //$"            where Id = {callBaseNumbers}" +
    $" where Phone = {number}" +
                    $"        )");

                    var cp = db.CallProjects.Where(e => e.Id == projectId).FirstOrDefault();
                    var oper = db.Operators.Where(e => e.Id == operator_Id).FirstOrDefault();
                    //if (oper == null)
                    //{
                    //    using (var context = new FusionPBXData())
                    //    {
                    //        var pNumber = context.v_extensions.Where(e => e.extension == operatorFS).FirstOrDefault();
                    //        supervisor = db.Supervisors.Where(e => e.OktellUserId == pNumber.extension_uuid).FirstOrDefault();
                    //    }
                    //}

                    var client = db.Clients.Add(new Client
                    {
                        Franchisee_Id = cp.Franchisee_Id,
                        CallProject_Id = projectId,
                        CallCenter_Id = oper.CallCenter_Id,
                        Clinic_Id = cp.Clinic_Id,
                        Operator_Id = operator_Id,
                        Supervisor_Id = oper.Supervisor_Id,
                        LastName = number,
                        FirstName = "-",
                        Patronymic = "-",
                        Name = number,
                        Phone = number,
                        CreationDate = DateTime.Now,
                        Status = ClientStatus.New,
                        Valid = false,
                        ClientService_Id = 1
                    });
                    db.SaveChanges();

                    db.ClientStatusHistories.Add(new ClientStatusHistory
                    {
                        Status = ClientStatus.New,
                        CreationDate = DateTime.Now,
                        Client_Id = client.Id
                    });
                    db.SaveChanges();

                    //if (supervisor != null)
                    //{
                    //    db.SupervisorCalls.Where( e=> e.)
                    //}

                    if (cc != null)
                    {
                        cc.Confirmed = true;
                        cc.Client_Id = client.Id;
                        db.SaveChanges();
                    }
                }
                return Content("Ok");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        class CBNumber
        {
            public long Id { get; set; }
            public long CallBase_Id { get; set; }
            public string Phone { get; set; }
        }

        CBNumber GetCBNumber(string number, long? projectId)
        {
            return db.Database.SqlQuery<CBNumber>($@"
				select  top(1) cbn.Id, cbn.CallBase_Id, cbn.Phone
				from    {db.DbName()}.dbo.CallbaseNumbers_{projectId} cbn
				join    {db.DbName()}.dbo.CallBases cb
				on      cbn.CallBase_Id = cb.Id
				where   cb.IsActive = 1
				and     cb.CallProject_Id = @CallProject_Id
				and     cbn.Phone = @Phone
				",
                new SqlParameter("@CallProject_Id", projectId),
                new SqlParameter("@Phone", number)).FirstOrDefault();
        }

        [HttpPost]
        public ActionResult AddAutoClient(string number, string operatorName, long? projectId, string urlSound, string descr)
        {
            try
            {
                NLog.Fluent.Log.Trace().Message($"number: {number} / operatorName: {operatorName} / projectId: {projectId} / urlSound: {urlSound} / descr: {descr}").Write();

                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                if (db.CallProjectTestNumbers.Any(e => e.Phone == number)) return Content("OK");  // тестовый дозвон

                var cbNumber = GetCBNumber(number, projectId);
                if (cbNumber != null)
                {
                    db.Database.ExecuteSqlCommand($@"
                    update  {db.DbName()}.dbo.CallbaseNumbers_{projectId}
                    set     Confirmed = 1
                    where   Id = @Id

                    update  {db.DbName()}.dbo.CallBases
                    set     LastAssignDate = getDate()
                    where   Id = @CallBase_Id
				    ",
                        new SqlParameter("@Id", cbNumber.Id),
                        new SqlParameter("@CallBase_Id", cbNumber.CallBase_Id));

                }

                var cp = db.CallProjects.AsNoTracking().Where(e => e.Id == projectId).FirstOrDefault();

                var oper = db.Operators.AsNoTracking().Where(e => e.Name == operatorName & e.CallProjectOperators.FirstOrDefault().CallProject_Id == projectId).FirstOrDefault();

                descr = descr.Replace("_", " ");

                var cc = db.OperatorCalls.Where(e => e.CallBaseNumber == number && e.CallProject_Id == projectId).OrderByDescending(e => e.Id).FirstOrDefault();

                var client = db.Clients.Add(new Client
                {
                    Franchisee_Id = cp.Franchisee_Id,
                    CallProject_Id = projectId,
                    CallJob_Id = cc?.CallJob_Id,
                    ScriptName = cc?.ScriptName,
                    CallCenter_Id = oper.CallCenter_Id,
                    Clinic_Id = cp.Clinic_Id,
                    Operator_Id = oper.Id,
                    LastName = number,
                    FirstName = "-",
                    Patronymic = "-",
                    Name = number,
                    Phone = number,
                    CreationDate = DateTime.Now,
                    Status = ClientStatus.New,
                    Valid = false,
                    ClientService_Id = 1,
                    Description = descr
                });

                if (cc != null)
                {
                    cc.URL = urlSound;
                    cc.Confirmed = true;
                    cc.Client_Id = client.Id;
                    cc.Operator_Id = oper.Id;
                }

                db.SaveChanges();

                db.ClientStatusHistories.Add(new ClientStatusHistory
                {
                    Status = ClientStatus.New,
                    CreationDate = DateTime.Now,
                    Client_Id = client.Id
                });
                db.SaveChanges();

               
                return Content(client.Id.ToString());
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpPost]
        public ActionResult UpdateAutoClient(long id, string descr)
        {
            try
            {
                NLog.Fluent.Log.Trace().Message($"clientId: {id} / descr: {descr}").Write();

                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);
                
                descr = descr.Replace("_", " ");

                var client = db.Clients.Where(e => e.Id == id).FirstOrDefault();

                if(client != null)
                {
                    client.Description = descr;
                    db.SaveChanges();
                }
                else
                {
                    return Content("Failed");
                }
                               
                return Content("Ok");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpPost]
        public ActionResult AddNewWebClient(string number, string operatorName, long? projectId, string urlSound, string descr)
        {
            try
            {
                var requestServer = HttpContext.Request.Params["SERVER_NAME"];
                var requestPort = HttpContext.Request.Params["SERVER_PORT"];
                NLog.Fluent.Log.Trace().Message($"serverName: {requestServer} / serverPort: {requestPort} / number: {number} / operator: {operatorName} / projectId: {projectId} / urlSound: {urlSound} / descr: {descr}").Write();

                if (ForbiddenAddClientRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                var cp = db.CallProjects.AsNoTracking().Where(e => e.Id == projectId).FirstOrDefault();
                var oper = db.Operators.AsNoTracking().Where(e => e.Name == operatorName).FirstOrDefault();

                var client = db.Clients.Add(new Client
                {
                    Franchisee_Id = cp.Franchisee_Id,
                    CallProject_Id = projectId,
                    CallCenter_Id = oper.CallCenter_Id,
                    Clinic_Id = cp.Clinic_Id,
                    Operator_Id = oper.Id,
                    LastName = number,
                    FirstName = "-",
                    Patronymic = "-",
                    Name = number,
                    Phone = number,
                    CreationDate = DateTime.Now,
                    Status = ClientStatus.New,
                    Valid = false,
                    ClientService_Id = 1,
                    Description = descr
                });
                db.SaveChanges();

                db.ClientStatusHistories.Add(new ClientStatusHistory
                {
                    Status = ClientStatus.New,
                    CreationDate = DateTime.Now,
                    Client_Id = client.Id
                });
                db.SaveChanges();

                var oc = db.OperatorCalls.Add(new OperatorCall
                {
                    Operator_Id = client.Operator_Id,
                    Client_Id = client.Id,
                    CallProject_Id = projectId,
                    TimeStart = DateTime.Now,
                    DialMethod = CallDialMethod.Robot,
                    CallBaseNumber = number,
                    Confirmed = true,
                    AbonentAnswerTime = 0,
                    URL = urlSound
                });
                db.SaveChanges();

                return Content("Ok");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpGet]
        [AllowAnonymous]
        public ActionResult AddVR_Record(long callProject_Id, string status, string phone, string question, string result, string uuid, string index)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                if (db.CallProjectTestNumbers.Any(e => e.Phone == phone)) return Content("OK");  // тестовый дозвон

                storage.Database.ExecuteSqlCommand($"insert	{storage.DbName()}.dbo.IvrAnswers ([CallProject_Id],[Uuid],[Phone],[Index],[Question],[Answer],[Value],[RecognitionDate],[RecognitionDateTime]) " +
 $"VALUES({callProject_Id}, '{uuid.ToString()}', '{phone}', {index}, '{question}', '{result}', '{status}', cast(getDate() as date), GETDATE())");

                return Content("OK");
                //storage.IvrAnswers.Add(new IvrAnswer()
                //{
                //    CallProject_Id = callProject_Id,
                //    RecognitionDateTime = DateTime.Now,
                //    Phone = phone,
                //    Question = question,
                //    Answer = result,
                //    Uuid = new Guid(uuid),
                //    Value = status,
                //    Index = Convert.ToInt32(index)

                //});

                //db.SaveChanges();

                //return Content("OK");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpPost]
        [AllowAnonymous]
        public ActionResult AddVR_RecordPost(long callProject_Id, string status, string phone, string question, string result, string uuid, string index, string urlSound)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                if (db.CallProjectTestNumbers.Any(e => e.Phone == phone)) return Content("OK");  // тестовый дозвон

                storage.Database.ExecuteSqlCommand($"insert	{storage.DbName()}.dbo.IvrAnswers ([CallProject_Id],[Uuid],[Phone],[Index],[Question],[Answer],[Value],[RecognitionDate],[RecognitionDateTime],[UrlSound]) " +
 $"VALUES({callProject_Id}, '{uuid.ToString()}', '{phone}', {index}, '{question}', '{result}', '{status}', cast(getDate() as date), GETDATE()), '{urlSound}'");

                return Content("OK");
                
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }


        [HttpGet]
        [AllowAnonymous]
        public ActionResult UpdateCallConnection(string uuid, string timeTransfer, string timeStop)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                NLog.Fluent.Log.Trace().Message($"uuid: {uuid} / timeTransfer: {timeTransfer} / timeStop: {timeStop}").Write();

                DateTime? TimeStop = null;
                DateTime? TimeTransfer = null;
                
                if (timeTransfer != null && timeTransfer != "")
                {
                    TimeTransfer = DateTime.ParseExact(timeTransfer, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();
                    storage.Database.ExecuteSqlCommand($"UPDATE	{storage.DbName()}.dbo.CallConnections SET TimeTransfer = '{TimeTransfer.Value.ToString("yyyy-MM-dd HH:mm:ss.fff")}' Where Uuid = '{uuid}'");
                }
               
                if (timeStop != null && timeStop != "")
                {
                    TimeStop = DateTime.ParseExact(timeStop, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();
                    storage.Database.ExecuteSqlCommand($"UPDATE	{storage.DbName()}.dbo.CallConnections SET TimeStop = '{TimeStop.Value.ToString("yyyy-MM-dd HH:mm:ss.fff")}' Where Uuid = '{uuid}'");
                }

                return Content("OK");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }

        }

        [HttpGet]
        [AllowAnonymous]
        public ActionResult CreateCallConnectionTest(string uuid, string from, string gate, string calledId, string callerId, string timeStart, string timeAnswer, string timeTransfer, string timeStop, int? connectionType/*, long? callEffort_Id*/)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                if (db.CallProjectTestNumbers.Any(e => e.Phone == callerId)) return Content("OK");  // тестовый дозвон

                NLog.Fluent.Log.Trace().Message($"uuid: {uuid} / from: {from} / gate: {gate} / calledId: {calledId} / callerId: {callerId} / timeStart: {timeStart} / timeAnswer: {timeAnswer} / timeTransfer: {timeTransfer} / timeStop: {timeStop} / connectionType: {connectionType}").Write();

                long? callEffort_Id = null;

                connectionType = connectionType.HasValue ? connectionType == 0 ? 1 : connectionType : 1;

                var sqlLong = new Func<long?, string>((value) => value.HasValue ? $"{value}" : "null");

                System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);

                if (callerId.Split('*').Count() > 1)
                {
                    callerId = callerId.Split('*')[1];
                }

                var TimeAnswer = dtDateTime.AddSeconds(Convert.ToDouble(timeAnswer.Substring(0, timeAnswer.Length - 6)/* + "," + answered_time.Substring(answered_time.Length - 6)*/)).ToLocalTime();
                var TimeStart = dtDateTime.AddSeconds(Convert.ToDouble(timeStart.Substring(0, timeStart.Length - 6) /*+ "," + created_time.Substring(created_time.Length - 6)*/)).ToLocalTime();
                //var TimeStop = dtDateTime.AddSeconds(Convert.ToDouble(timeStop.Substring(0, timeStop.Length - 6) /*+ "," + created_time.Substring(created_time.Length - 6)*/)).ToLocalTime();
                DateTime TimeStop = DateTime.ParseExact(timeStop, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();

                //DateTime TimeStart = DateTime.ParseExact(timeStart, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();
                //DateTime TimeAnswer = DateTime.ParseExact(timeAnswer, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();
                //DateTime TimeStop = DateTime.ParseExact(timeStop, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();

                //DateTime? TimeTransfer = null;

                var server = db.CallServers.FirstOrDefault(e => e.Code.ToLower() == from.ToLower());

                if (timeTransfer != "0" && timeTransfer != null)
                {
                    var TimeTransfer = DateTime.ParseExact(timeTransfer, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();
                    //var TimeTransfer = dtDateTime.AddSeconds(Convert.ToDouble(timeTransfer.Substring(0, timeTransfer.Length - 6) /*+ "," + created_time.Substring(created_time.Length - 6)*/)).ToLocalTime();
                    //TimeTransfer = DateTime.ParseExact(timeTransfer, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();

                    storage.Database.ExecuteSqlCommand($"insert	{storage.DbName()}.dbo.CallConnections   ([Uuid],[CallServer_Id],[CallEffort_Id],[DateStart],[TimeStart],[TimeAnswer],[TimeTransfer],[TimeStop],[CalledId],[CallerId],[ConnectionType],[Gate]) " +
      $"VALUES('{uuid.ToString()}', {server.Id}, {sqlLong(callEffort_Id)}, '{TimeStart.ToString("yyyy-MM-dd")}', '{TimeStart.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{TimeAnswer.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{TimeTransfer.ToString("yyyy-MM-dd HH:mm:ss.fff")}','{TimeStop.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{calledId}', '{callerId}', {connectionType}, '{gate}')");
                }
                else
                {
                    storage.Database.ExecuteSqlCommand($"insert	{storage.DbName()}.dbo.CallConnections   ([Uuid],[CallServer_Id],[CallEffort_Id],[DateStart],[TimeStart],[TimeAnswer],[TimeStop],[CalledId],[CallerId],[ConnectionType],[Gate]) " +
        $"VALUES('{uuid.ToString()}', {server.Id}, {sqlLong(callEffort_Id)}, '{TimeStart.ToString("yyyy-MM-dd")}', '{TimeStart.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{TimeAnswer.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{TimeStop.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{calledId}', '{callerId}', {connectionType}, '{gate}')");
                }


                //storage.CallConnections.Add(new Storage.CallConnection
                //{
                //    CallServer_Id = server.Id,
                //    Gate = gate,
                //    CalledId = calledId,
                //    CallerId = callerId,
                //    TimeStart = TimeStart,
                //    TimeAnswer = TimeAnswer,
                //    TimeTransfer = TimeTransfer,
                //    TimeStop = TimeStop,
                //    Uuid = new Guid(uuid),
                //    ConnectionType = CallConnectionType.Outgoing
                //});

                //storage.SaveChanges();

                return Content("OK");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }

        }

      //  [HttpGet]
      //  [AllowAnonymous]
      //  public ContentResult CreateCallConnection(string uuid, string from, string gate, string calledId, string callerId, string timeStart, string timeAnswer, string timeTransfer, string timeStop, int? connectionType /*, long? callEffort_Id*/)
      //  {
      //      try
      //      {
      //          if (db.CallProjectTestNumbers.Any(e => e.Phone == callerId)) return Content("OK");  // тестовый дозвон

      //          NLog.Fluent.Log.Trace().Message($"uuid: {uuid} / from: {from} / gate: {gate} / calledId: {calledId} / callerId: {callerId} / timeStart: {timeStart} / timeAnswer: {timeAnswer} / timeTransfer: {timeTransfer} / timeStop: {timeStop} / connectionType: {connectionType}").Write();

      //          long? callEffort_Id = null;

      //          connectionType = connectionType.HasValue ? connectionType == 0 ? 1 : connectionType : 1;

      //          var sqlLong = new Func<long?, string>((value) => value.HasValue ? $"{value}" : "null");

      //          DateTime TimeStart = DateTime.ParseExact(timeStart, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();
      //          DateTime TimeAnswer = DateTime.ParseExact(timeAnswer, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();
      //          DateTime TimeStop = DateTime.ParseExact(timeStop, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();

      //          DateTime? TimeTransfer = null;

      //          var server = db.CallServers.FirstOrDefault(e => e.Code.ToLower() == from.ToLower());

      //          if (timeTransfer != null && timeTransfer != "")
      //          {
      //              TimeTransfer = DateTime.ParseExact(timeTransfer, "yyyy-MM-ddTHH:mm:ss.ffffff", System.Globalization.CultureInfo.InvariantCulture).ToLocalTime();

      //              storage.Database.ExecuteSqlCommand($"insert	{storage.DbName()}.dbo.CallConnections   ([Uuid],[CallServer_Id],[CallEffort_Id],[DateStart],[TimeStart],[TimeAnswer],[TimeTransfer],[TimeStop],[CalledId],[CallerId],[ConnectionType],[Gate]) " +
      //$"VALUES('{uuid.ToString()}', {server.Id}, {sqlLong(callEffort_Id)}, '{TimeStart.ToString("yyyy-MM-dd")}', '{TimeStart.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{TimeAnswer.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{TimeTransfer.Value.ToString("yyyy-MM-dd HH:mm:ss.fff")}','{TimeStop.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{calledId}', '{callerId}', {connectionType}, '{gate}')");
      //          }
      //          else
      //          {
      //              storage.Database.ExecuteSqlCommand($"insert	{storage.DbName()}.dbo.CallConnections   ([Uuid],[CallServer_Id],[CallEffort_Id],[DateStart],[TimeStart],[TimeAnswer],[TimeStop],[CalledId],[CallerId],[ConnectionType],[Gate]) " +
      //  $"VALUES('{uuid.ToString()}', {server.Id}, {sqlLong(callEffort_Id)}, '{TimeStart.ToString("yyyy-MM-dd")}', '{TimeStart.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{TimeAnswer.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{TimeStop.ToString("yyyy-MM-dd HH:mm:ss.fff")}', '{calledId}', '{callerId}', {connectionType}, '{gate}')");
      //          }


      //          //storage.CallConnections.Add(new Storage.CallConnection
      //          //{
      //          //    CallServer_Id = server.Id,
      //          //    Gate = gate,
      //          //    CalledId = calledId,
      //          //    CallerId = callerId,
      //          //    TimeStart = TimeStart,
      //          //    TimeAnswer = TimeAnswer,
      //          //    TimeTransfer = TimeTransfer,
      //          //    TimeStop = TimeStop,
      //          //    Uuid = new Guid(uuid),
      //          //    ConnectionType = CallConnectionType.Outgoing
      //          //});

      //          //storage.SaveChanges();

      //          return Content("OK");
      //      }
      //      catch (Exception ex)
      //      {
      //          return Content(ex.Message);
      //      }

      //  }

        [HttpGet]
        [AllowAnonymous]
        public ActionResult UpdateLineStates(string from, string code, int totalCount, int callCount, int dialCount, int readyCount)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                NLog.Fluent.Log.Trace().Message($"from: {from} / code: {code} / totalCount: {totalCount} / callCount: {callCount} / readyCount: {readyCount}").Write();

                var codes = code.Split('_');
                var route_prefix = codes.Length == 2 ? codes[0] : "";
                var route_code = codes.Length == 2 ? codes[1] : code;

                var server = db.CallServers.FirstOrDefault(e => e.Code.ToLower() == from.ToLower());
                var provider = db.Providers.FirstOrDefault(e => e.Code.ToLower() == route_code.ToLower());

                if (server != null && provider != null)
                {
                    var entity = new CallServerLineState
                    {
                        CallServer_Id = server.Id,
                        Provider_Id = provider.Id,
                        Prefix = route_prefix,
                        TotalCount = totalCount,
                        CallCount = callCount,
                        DialCount = dialCount,
                        ReadyCount = readyCount,
                        LastAssignDate = DateTime.Now
                    };

                    if (db.CallServerLineStates.Any(e => e.CallServer_Id == server.Id && e.Provider_Id == provider.Id && e.Prefix == route_prefix))
                    {
                        db.CallServerLineStates.Attach(entity);
                        db.Entry(entity).State = EntityState.Modified;
                    }
                    else
                    {
                        db.CallServerLineStates.Add(entity);
                    }

                    db.SaveChanges();
                }

                db.Database.ExecuteSqlCommand($"delete {db.DbName()}.dbo.CallServerLineStates where LastAssignDate is null or LastAssignDate < @LastAssignDate", new SqlParameter("@LastAssignDate", DateTime.Now.AddSeconds(-60)));

                return Content("OK");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpGet]
        [AllowAnonymous]
        public ActionResult CreateCallbackEffort(long callProject_Id, string phone)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                if (db.CallProjectTestNumbers.Any(e => e.Phone == phone)) return Content("OK");  // тестовый дозвон

                var callEffort_Id = db.Database.SqlQuery<long>($@"
					insert	{storage.DbName()}.dbo.CallEfforts(CallProject_Id, CallBase_Id, DateStart, TimeStart, DialMethod, CallBaseNumber)
					output	inserted.Id
                    values  (@CallProject_Id, null, cast(getDate() as date), getDate(), @DialMethod, @CallBaseNumber)
					",
                    new SqlParameter("@CallProject_Id", callProject_Id),
                    new SqlParameter("@DialMethod", (int)CallDialMethod.Incoming),
                    new SqlParameter("@CallBaseNumber", phone)).First();

                return Content(callEffort_Id.ToString());
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpGet]
        [AllowAnonymous]
        public ActionResult CreateOperatorCall(long callProject_Id, long? callBaseNumber_Id, string callBaseNumber, string connection_Id, string gate)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                if (db.CallProjectTestNumbers.Any(e => e.Phone == callBaseNumber)) return Content("OK");  // тестовый дозвон

                NLog.Fluent.Log.Trace().Message($"callProject_Id: {callProject_Id} / callBaseNumber_Id: {callBaseNumber_Id} / callBaseNumber: {callBaseNumber} / connection_Id: {connection_Id} / gate: {gate}").Write();

                var operatorCall_Id = callBaseNumber_Id.HasValue ?
                    
                    db.Database.SqlQuery<long>($@"
                        insert	{db.DbName()}.dbo.OperatorCalls(Operator_Id, OktellConnectionId, Client_Id, CallProject_Id, CallBase_Id, TimeStart, Duration, DialMethod, DialStatus, CallBaseNumber, Confirmed, AbonentAnswerTime, Gate)
                        output	inserted.Id
                        select	null, @OktellConnectionId, null, @CallProject_Id, CallBase_Id, getDate(), null, {(int)CallDialMethod.Auto}, {(int)CallDialStatus.Success}, Phone, 0, 0, @Gate
                        from	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id} where Id = @Id
                        ",
                        new SqlParameter("@Id", callBaseNumber_Id),
                        new SqlParameter("@CallProject_Id", callProject_Id),
                        new SqlParameter("@OktellConnectionId", string.IsNullOrEmpty(connection_Id) ? DBNull.Value : (object)Guid.Parse(connection_Id)),
                        new SqlParameter("@Gate", gate ?? string.Empty)).First() :

                    db.Database.SqlQuery<long>($@"
                        insert	{db.DbName()}.dbo.OperatorCalls(Operator_Id, OktellConnectionId, Client_Id, CallProject_Id, CallBase_Id, TimeStart, Duration, DialMethod, DialStatus, CallBaseNumber, Confirmed, AbonentAnswerTime, Gate)
                        output	inserted.Id
                        select	null, @OktellConnectionId, null, @CallProject_Id, null, getDate(), null, {(int)CallDialMethod.Incoming}, {(int)CallDialStatus.Success}, @CallBaseNumber, 0, 0, @Gate
                        ",
                        new SqlParameter("@CallBaseNumber", callBaseNumber),
                        new SqlParameter("@CallProject_Id", callProject_Id),
                        new SqlParameter("@OktellConnectionId", string.IsNullOrEmpty(connection_Id) ? DBNull.Value : (object)Guid.Parse(connection_Id)),
                        new SqlParameter("@Gate", gate ?? string.Empty)).First();

                NLog.Fluent.Log.Trace().Message($"callBaseNumber: {callBaseNumber} / operatorCall_Id: {operatorCall_Id}").Write();
                return Content(operatorCall_Id.ToString());
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpGet]
        [AllowAnonymous]
        public ActionResult GetFreeOperator(string uuid, string projectNumber, string phone)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                var ops = db.OperatorCalls.Where(e => new Guid(uuid) == e.Uuid).FirstOrDefault();
                if (ops != null)
                {
                    var o = ops.Operator;
                    o.State = OperatorState.Online;
                    o.Reason = OperatorStateReason.OperatorLogin;
                    db.SaveChanges();
                    return Content("BUSSY");
                }

                var callProject = db.CallProjects.Where(e => projectNumber == e.LineNumber).FirstOrDefault();
                var readyOperators = callProject.CallProjectOperators.Where(e => e.Operator.State == OperatorState.Online).Select(e => e.Operator_Id).ToArray();

                if (readyOperators.Count() == 0)
                {
                    return Content("NOT OPERATOR");
                }

                Random rnd = new Random();
                var op = readyOperators[rnd.Next(readyOperators.Count())];

                db.OperatorCalls.Add(
                    new OperatorCall
                    {
                        Uuid = new Guid(uuid),
                        CallProject_Id = callProject.Id,
                        TimeStart = DateTime.Now,
                        CallBaseNumber = phone.Split('*')[1],
                        Operator_Id = op,
                        DtmfQuery = null
                    });

                db.Operators.Where(e => e.Id == op).FirstOrDefault().State = OperatorState.Call;
                db.Operators.Where(e => e.Id == op).FirstOrDefault().Reason = OperatorStateReason.Call;
                db.SaveChanges();

                var osh = db.OperatorStateHistories.Where(e => e.Operator_Id == op).OrderByDescending(e => e.CreationDate).FirstOrDefault();
                return Content(osh.LineNumber);
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpPost]
        public ActionResult AddNumberToBlackList(string number)
        {
            if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

            if (db.CallProjectTestNumbers.Any(e => e.Phone == number)) return Content("OK");  // тестовый дозвон

            db.StopListNumbers.Add(new StopListNumber { Phone = number.Substring(number.Length - 11), CreationDate = DateTime.Now, CreationMode = CreationMode.OperatorAuto, CreatorName = "Робот" });
            db.SaveChanges();

            return Content("Ok");
        }


        //[HttpGet]
        //[AllowAnonymous]
        //public ContentResult SetFreeOperator(string uuid)
        //{
        //    try
        //    {
        //        var ope = db.OperatorCalls.Where(e => new Guid(uuid) == e.Uuid).FirstOrDefault();
        //        var operat = db.Operators.Where(e => e.Id == ope.Id).FirstOrDefault();
        //        operat.State = OperatorState.Online;
        //        operat.Reason = OperatorStateReason.OperatorLogin;
        //        db.SaveChanges();

        //        return Content("Ok");
        //    }
        //    catch (Exception ex)
        //    {
        //        return Content(ex.Message);
        //    }
        //}

        [HttpGet]
        [AllowAnonymous]
        public ActionResult UpdateNumberState(long callProject_Id, long callBaseNumber_Id, string state)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                NLog.Fluent.Log.Trace().Message($"callProject_Id: {callProject_Id} / callBaseNumber_Id: {callBaseNumber_Id} / state: {state}").Write();

                switch (state.ToUpper())
                {
                    case "C":
                        db.Database.ExecuteSqlCommand($@"
							update	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id}
							set     State = 'C',
									Result = 1,
                                    LastAttemptDate = getDate()
							where   Id = @id
							", new SqlParameter("@id", callBaseNumber_Id));
                        break;

                    case "T":
                        db.Database.ExecuteSqlCommand($@"
							update	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id}
							set     State = case when AttemptsQty < (select DialEffortLimit from {db.DbName()}.dbo.CallProjects where Id = @callProject_Id) then 'T' else 'C' end,
									Result = 0,
                                    LastAttemptDate = getDate()
							where   Id = @id
							",
                            new SqlParameter("@id", callBaseNumber_Id),
                            new SqlParameter("@callProject_Id", callProject_Id));
                        break;

                    case "L":
                        db.Database.ExecuteSqlCommand($@"
							update	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id}
							set     State = 'L',
									AttemptsQty = AttemptsQty - 1,
                                    LastAttemptDate = getDate()
							where   Id = @id
							", new SqlParameter("@id", callBaseNumber_Id));
                        break;

                    case "P":
                        var callEffort_Id = db.Database.SqlQuery<long>($@"
							update	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id}
							set     State = 'P',
									LastAttemptDate = getDate(),
									AttemptsQty = isnull(AttemptsQty, 0) + 1
							where   Id = @id

							insert	{storage.DbName()}.dbo.CallEfforts(CallProject_Id, CallBase_Id, DateStart, TimeStart, DialMethod, CallBaseNumber)
							output	inserted.Id
							select	{callProject_Id}, CallBase_Id, cast(getDate() as date), getDate(), {(int)CallDialMethod.Auto}, Phone
							from	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id} where Id = @id
							",
                            new SqlParameter("@id", callBaseNumber_Id)).First();
                        return Content(callEffort_Id.ToString());
                }
                return Content("OK");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        //[HttpGet]
        //[AllowAnonymous]
        //public ContentResult UpdateCheckBaseNumberState(long checkProject_Id, long checkBaseNumber_Id, string state)
        //{
        //    try
        //    {
        //        NLog.Fluent.Log.Trace().Message($"checkProject_Id: {checkProject_Id} / checkBaseNumber_Id: {checkBaseNumber_Id} / state: {state}").Write();

        //        var date = DateTime.Now;

        //        switch (state.ToUpper())
        //        {
        //            case "L":
        //                db.Database.ExecuteSqlCommand($@"
        //                    update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
        //                    set     Status = null, LastAttemptDate = @date
        //                    where   Id = @id
        //                    ", new SqlParameter("@id", checkBaseNumber_Id),
        //                    new SqlParameter("@date", date));
        //                break;

        //            case "P":
        //                db.Database.ExecuteSqlCommand($@"
        //                    update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
        //                    set     Status = 'Процессинг', LastAttemptDate = @date
        //                    where   Id = @id
        //                    ", new SqlParameter("@id", checkBaseNumber_Id),
        //                    new SqlParameter("@date", date));
        //                break;

        //            case "C":
        //                db.Database.ExecuteSqlCommand($@"
        //                    update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
        //                    set     Status = 'Ответ', LastAttemptDate = @date
        //                    where   Id = @id
        //                    ", new SqlParameter("@id", checkBaseNumber_Id),
        //                    new SqlParameter("@date", date));
        //                break;

        //            case "F":
        //                db.Database.ExecuteSqlCommand($@"
        //                    update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
        //                    set     Status = 'БыстрОтв', LastAttemptDate = @date
        //                    where   Id = @id
        //                    ", new SqlParameter("@id", checkBaseNumber_Id),
        //                    new SqlParameter("@date", date));
        //                break;

        //            case "T":
        //                db.Database.ExecuteSqlCommand($@"
        //                    update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
        //                    set     Status = 'Не отвечает', LastAttemptDate = @date
        //                    where   Id = @id
        //                    ", new SqlParameter("@id", checkBaseNumber_Id),
        //                    new SqlParameter("@date", date));
        //                break;

        //            case "B":
        //                db.Database.ExecuteSqlCommand($@"
        //                    update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
        //                    set     Status = 'Занято', LastAttemptDate = @date
        //                    where   Id = @id
        //                    ", new SqlParameter("@id", checkBaseNumber_Id),
        //                    new SqlParameter("@date", date));
        //                break;

        //            case "E":
        //                db.Database.ExecuteSqlCommand($@"
        //                    update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
        //                    set     Status = 'Ошибка', LastAttemptDate = @date
        //                    where   Id = @id
        //                    ", new SqlParameter("@id", checkBaseNumber_Id),
        //                    new SqlParameter("@date", date));
        //                break;
        //        }
        //        return Content("OK");
        //    }
        //    catch (Exception ex)
        //    {
        //        return Content(ex.Message);
        //    }
        //}

       // [HttpGet]
       // [AllowAnonymous]
       // public ContentResult UpdateNumberState_maxversion(long callProject_Id, long callBaseNumber_Id, string state)
       // {
       //     try
       //     {
       //         switch (state.ToUpper())
       //         {
       //             case "C":
       //                 db.Database.ExecuteSqlCommand($@"
       //                     update	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id}
       //                     set     State = 'C',
		     //                       Result = 1
       //                     where   Id = @id
       //                     ", new SqlParameter("@id", callBaseNumber_Id));
       //                 break;

       //             case "T":
       //                 db.Database.ExecuteSqlCommand($@"
       //                     update	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id}
       //                     set     State = case when AttemptsQty < 3 then 'T' else 'C' end,
		     //                       Result = 0
       //                     where   Id = @id
       //                     ", new SqlParameter("@id", callBaseNumber_Id));
       //                 break;

       //             case "L":
       //                 db.Database.ExecuteSqlCommand($@"
       //                     update	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id}
       //                     set     State = 'L',
		     //                       AttemptsQty = AttemptsQty - 1
       //                     where   Id = @id
       //                     ", new SqlParameter("@id", callBaseNumber_Id));
       //                 break;

       //             case "P":
       //                 var callEffort_Id = db.Database.SqlQuery<long>($@"
							//update	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id}
							//set     State = 'P',
							//		LastAttemptDate = getDate(),
							//		AttemptsQty = isnull(AttemptsQty, 0) + 1
							//where   Id = @id

       //                     insert	{db.DbName()}.dbo.OperatorCalls(Operator_Id, OktellConnectionId, Client_Id, CallProject_Id, CallBase_Id, TimeStart, Duration, DialMethod, DialStatus, CallBaseNumber, Confirmed, AbonentAnswerTime)
       //                     output	inserted.Id
       //                     select	null, null, null, {callProject_Id}, CallBase_Id, getDate(), null, 1, 0, Phone, 0, 0
       //                     from	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id} where Id = @id

							//insert	{storage.DbName()}.dbo.CallEfforts(CallProject_Id, CallBase_Id, TimeStart, DialMethod, CallBaseNumber)
							//output	inserted.Id
							//select	{callProject_Id}, CallBase_Id, getDate(), {(int)CallDialMethod.Auto}, Phone
							//from	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id} where Id = @id
							//",
       //                     new SqlParameter("@id", callBaseNumber_Id)).First();
       //                 return Content(callEffort_Id.ToString());
       //         }
       //         return Content("OK");
       //     }
       //     catch (Exception ex)
       //     {
       //         return Content(ex.Message);
       //     }
       // }

        [HttpGet]
        [AllowAnonymous]
        public ActionResult UpdateCheckBaseNumberStateFS(long checkProject_Id, long checkBaseNumber_Id, string created_time, string answered_time, string status, int? code, int? codefs)
        {
            try
            {
                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                NLog.Fluent.Log.Trace().Message($"checkProject_Id: {checkProject_Id} / checkBaseNumber_Id: {checkBaseNumber_Id} / status: {status} / code: {code} / codefs: {codefs}").Write();

                //  var value = db.Database.SqlQuery<string>($@"SELECT Status FROM {db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id} WHERE Id = {checkBaseNumber_Id}").FirstOrDefault();
                //   if ((value != "" && value != null) && answered_time == "0") return Content("OK");

                //System.DateTime dtDateTime = new DateTime(1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc);

                //  var TimeAnswer = dtDateTime.AddSeconds(Convert.ToDouble(answered_time.Substring(0, answered_time.Length - 6)/* + "," + answered_time.Substring(answered_time.Length - 6)*/)).ToLocalTime();
                //  var TimeStart = dtDateTime.AddSeconds(Convert.ToDouble(created_time.Substring(0, created_time.Length - 6) /*+ "," + created_time.Substring(created_time.Length - 6)*/)).ToLocalTime();

                if (status.Contains("USER_BUSY"))
                {
                    db.Database.ExecuteSqlCommand($@"
						update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
						set     Status = 'Занято',
								StatusCode = @code,
								StatusMessage = @message,
								LastAttemptDate = getDate()
						where   Id = @id
						",
                        new SqlParameter("@id", checkBaseNumber_Id),
                        new SqlParameter("@code", code),
                        new SqlParameter("@message", status));
                }
                else if (status.Contains("NORMAL_CLEARING") && Convert.ToSingle(answered_time) != 0)
                {
                    //if ((Convert.ToSingle(answered_time) - Convert.ToSingle(created_time)) / 1000000 > 7)
                    //  {
                    db.Database.ExecuteSqlCommand($@"
						update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
						set     Status = 'Ответ',
								StatusCode = @code,
								StatusMessage = @message,
								LastAttemptDate = getDate()
						where   Id = @id
						",
                        new SqlParameter("@id", checkBaseNumber_Id),
                        new SqlParameter("@code", code),
                        new SqlParameter("@message", status));
                    //}
                    //else
                    //{
                    //    db.Database.ExecuteSqlCommand($@"
                    //        update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
                    //        set     Status = 'БыстрОтв'
                    //        where   Id = @id
                    //        ", new SqlParameter("@id", checkBaseNumber_Id));
                    //    return Content("OK");
                    //}
                }
                else if (status.Contains("NO_ANSWER") || status.Contains("CALL_REJECTED") || status == "0")
                {
                    db.Database.ExecuteSqlCommand($@"
						update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
						set     Status = 'Не отвечает',
								StatusCode = @code,
								StatusMessage = @message,
								LastAttemptDate = getDate()
						where   Id = @id
						",
                        new SqlParameter("@id", checkBaseNumber_Id),
                        new SqlParameter("@code", code),
                        new SqlParameter("@message", status));
                }

                else
                {
                    db.Database.ExecuteSqlCommand($@"
						update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
						set     Status = 'Ошибка',
								StatusCode = @code,
								StatusMessage = @message,
								LastAttemptDate = getDate()
						where   Id = @id
						",
                        new SqlParameter("@id", checkBaseNumber_Id),
                        new SqlParameter("@code", code),
                        new SqlParameter("@message", status));
                }

                return Content("OK");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpGet]
        [AllowAnonymous]
        public ActionResult AddCallCheckAnswer_Record(long checkProject_Id, string phone, string answer, string value, string uuid)
        {
            try
            {

                if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

 //               storage.Database.ExecuteSqlCommand($"insert	{storage.DbName()}.dbo.CallCheckAnswer ([CheckProject_Id],[Uuid],[Phone],[Answer],[Value],[RecognitionDateTime]) " +
 //$"VALUES({checkProject_Id}, '{uuid.ToString()}', '{phone}', '{answer}', '{value}', GETDATE())");

 //               return Content("OK");
                storage.CallCheckAnswers.Add(new stor.CallCheckAnswer()
                {
                    CheckProject_Id = checkProject_Id,
                    Uuid = new Guid(uuid),
                    Phone = phone,
                    Answer = answer,
                    Value = value,
                    RecognitionDateTime = DateTime.Now
                });

                db.SaveChanges();

                return Content("OK");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        public class LoginOperatorClass
        {
            public string login { get; set; }
            
            public string name { get; set; }

            public string sip_acc { get; set; }

            public string sip_pass { get; set; }
        }

        [HttpPost]
        [AllowAnonymous]
        public ActionResult LoginOperatorPlace(string login, string pass)
        {
            try
            {

                //if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                var oper = db.Operators.Where(e => e.Email == login && e.AuthCode == pass).FirstOrDefault();
                
                if(oper != null)
                {
                    var user = oper.User;

                    LoginOperatorClass entity = new LoginOperatorClass
                    {
                        login = "true",
                        name = oper.Name,
                        sip_acc = user.SipLogin,
                        sip_pass = user.SipPassword
                    };

                    return Json(entity, JsonRequestBehavior.AllowGet);
                }
                else
                {
                    LoginOperatorClass entity = new LoginOperatorClass
                    {
                        login = "false"
                    };

                    return Json(entity, JsonRequestBehavior.AllowGet);
                }

            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpPost]
        [AllowAnonymous]
        public ActionResult GetClientsOperatorPlace(string login, string pass)
        {
            try
            {

                //if (ForbiddenRequest) return new HttpStatusCodeResult(HttpStatusCode.Forbidden);

                var oper = db.Operators.Where(e => e.Email == login && e.AuthCode == pass).FirstOrDefault();

                if (oper != null)
                {
                    var user = oper.User;

                    var clients = db.Clients.Where(c => c.Operator.Name.ToLower() == oper.Name.ToLower() || c.Status == ClientStatus.Callback).Select(e => new ClientForOperatorPlace
                    {
                        Id = e.Id,
                        Franchisee_Id = e.Franchisee_Id,
                        CallProject_Id = e.CallProject_Id,
                        CallJob_Id = e.CallJob_Id,
                        ScriptName = e.ScriptName,
                        Name = e.Name,
                        LastName = e.LastName,
                        FirstName = e.FirstName,
                        Patronymic = e.Patronymic,
                        BirthDate = e.BirthDate.ToString(),
                        Phone = e.Phone,
                        Age = e.Age,
                        Height = e.Height,
                        Weight = e.Weight,
                        Gender = e.Gender,
                        Geo = e.Geo,
                        EmploymentStatus = e.EmploymentStatus,
                        Limit = e.Limit,
                        CreationDate = e.CreationDate.ToString(),
                        PresentationDate = e.PresentationDate.ToString(),
                        PresentationDate1 = e.PresentationDate1.ToString(),
                        PresentationDate2 = e.PresentationDate2.ToString(),
                        PresentationDate3 = e.PresentationDate3.ToString(),
                        RecallTime = e.RecallTime.ToString(),
                        ClientService_Id = e.ClientService_Id,
                        Description = e.Description,
                        Review = e.Review,
                        ReviewOk = e.ReviewOk,
                        CallCenter_Id = e.CallCenter_Id,
                        Operator_Id = e.Operator_Id,
                        Supervisor_Id = e.Supervisor_Id,
                        ShiftSupervisor_Id = e.ShiftSupervisor_Id,
                        Coordinator_Id = e.Coordinator_Id,
                        Coach_Id = e.Coach_Id,
                        Manager_Id = e.Manager_Id,
                        Promoter_Id = e.Promoter_Id,
                        Status = e.Status,
                        StatusGPV = e.StatusGPV,
                        Valid = e.Valid,
                        SmsBan = e.SmsBan,
                        ClinicPatient_Id = e.ClinicPatient_Id,
                        ReturnDate = e.ReturnDate.ToString(),
                        ETicketNumber = e.ETicketNumber,
                        Fssp = e.Fssp,
                        NeedScale = e.NeedScale,
                        ReviewTMGPV = e.ReviewTMGPV,
                        CommentTMGPV = e.CommentTMGPV,
                        CommentCoordinatorGPV = e.CommentCoordinatorGPV,
                        Office_Id = e.Office_Id,
                        ClientSource_Id = e.ClientSource_Id
                    });
                    //string json = JsonConvert.SerializeObject(clients);

                    return Json(clients, JsonRequestBehavior.AllowGet);
                }
                else
                {
                    return new HttpStatusCodeResult(HttpStatusCode.Forbidden);
                }

            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        class ClientForOperatorPlace
        {
            public long Id { get; set; }

            /// <summary>
            /// Франчайзи
            /// </summary>
            public long Franchisee_Id { get; set; }

            /// <summary>
            /// Проект дозвона
            /// </summary>
            public long? CallProject_Id { get; set; }

            /// <summary>
            /// Задача дозвона
            /// </summary>
            public long? CallJob_Id { get; set; }

            /// <summary>
            /// Имя сценария
            /// </summary>
            public string ScriptName { get; set; }

            /// <summary>
            /// Ф.И.О.
            /// </summary>
            public string Name { get; set; }

            /// <summary>
            /// Фамилия
            /// </summary>
            public string LastName { get; set; }

            /// <summary>
            /// Имя
            /// </summary>
            public string FirstName { get; set; }

            /// <summary>
            /// Отчество
            /// </summary>
            public string Patronymic { get; set; }

            /// <summary>
            /// Дата рождения
            /// </summary>
            public string BirthDate { get; set; }

            /// <summary>
            /// Номер телефона
            /// </summary>
            public string Phone { get; set; }

            /// <summary>
            /// Возраст, полных лет
            /// </summary>
            public int? Age { get; set; }

            /// <summary>
            /// Рост, см
            /// </summary>
            public int? Height { get; set; }

            /// <summary>
            /// Масса, кг
            /// </summary>
            public int? Weight { get; set; }

            /// <summary>
            /// Пол
            /// </summary>
            public Common_Gender? Gender { get; set; }

            /// <summary>
            /// Георафия
            /// </summary>
            public ClientGeo? Geo { get; set; }

            /// <summary>
            /// Занятость
            /// </summary>
            public ClientEmploymentStatus? EmploymentStatus { get; set; }

            /// <summary>
            /// Лимит
            /// </summary>
            public int? Limit { get; set; }

            /// <summary>
            /// Дата/время создания записи
            /// </summary>
            public string CreationDate { get; set; }

            /// <summary>
            /// Дата презентации
            /// </summary>
            public string PresentationDate { get; set; }

            /// <summary>
            /// Дата/время презентации 1
            /// </summary>
            public string PresentationDate1 { get; set; }

            /// <summary>
            /// Дата/время презентации 2
            /// </summary>
            public string PresentationDate2 { get; set; }

            /// <summary>
            /// Дата/время презентации 3
            /// </summary>
            public string PresentationDate3 { get; set; }

            /// <summary>
            /// Время перезвона
            /// </summary>
            public string RecallTime { get; set; }

            /// <summary>
            /// Вид услуги
            /// </summary>
            public long ClientService_Id { get; set; }

            /// <summary>
            /// Примечание
            /// </summary>
            public string Description { get; set; }

            /// <summary>
            /// Комментарий КЦ
            /// </summary>
            public string Review { get; set; }

            /// <summary>
            /// Рецензия ОП
            /// </summary>
            public string ReviewOk { get; set; }

            /// <summary>
            /// Колл-центр
            /// </summary>
            public long? CallCenter_Id { get; set; }

            /// <summary>
            /// Оператор
            /// </summary>
            public long? Operator_Id { get; set; }

            /// <summary>
            /// Супервайзер
            /// </summary>
            public long? Supervisor_Id { get; set; }

            /// <summary>
            /// Старший смены
            /// </summary>
            public long? ShiftSupervisor_Id { get; set; }

            /// <summary>
            /// Координатор
            /// </summary>
            public long? Coordinator_Id { get; set; }

            /// <summary>
            /// Тренер
            /// </summary>
            public long? Coach_Id { get; set; }

            /// <summary>
            /// Менеджер
            /// </summary>
            public long? Manager_Id { get; set; }

            /// <summary>
            /// Промоутер
            /// </summary>
            public long? Promoter_Id { get; set; }

            /// <summary>
            /// Статус
            /// </summary>
            public ClientStatus Status { get; set; }

            /// <summary>
            /// Статус ГПВ
            /// </summary>
            public ClientStatusGPV? StatusGPV { get; set; }

            /// <summary>
            /// Зачет
            /// </summary>
            public bool Valid { get; set; }

            /// <summary>
            /// Запрет СМС-уведомлений
            /// </summary>
            public bool SmsBan { get; set; }

            /// <summary>
            /// Пациент
            /// </summary>
            public long? ClinicPatient_Id { get; set; }

            /// <summary>
            /// Дата возврата
            /// </summary>
            public string ReturnDate { get; set; }

            /// <summary>
            /// Номер электронного талона
            /// </summary>
            public string ETicketNumber { get; set; }

            /// <summary>
            /// Шкала потребности
            /// </summary>
            public NeedScales? NeedScale { get; set; }

            /// <summary>
            /// ФССП
            /// </summary>
            public FSSPStatus? Fssp { get; set; }

            /// <summary>
            /// Рецензия ТМ ГПВ
            /// </summary>
            public string ReviewTMGPV { get; set; }

            /// <summary>
            /// Комментарий ТМ ГПВ
            /// </summary>
            public string CommentTMGPV { get; set; }

            /// <summary>
            /// Комментарий координатора ГПВ
            /// </summary>
            public string CommentCoordinatorGPV { get; set; }

            /// <summary>
            /// Офис
            /// </summary>
            public long? Office_Id { get; set; }

            /// <summary>
            /// Источник
            /// </summary>
            public long? ClientSource_Id { get; set; }
        }
    }
}