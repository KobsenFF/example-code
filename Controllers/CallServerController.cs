using bz.iteam.crm.data;
using bz.iteam.crm.docs;
using bz.iteam.crm.locales;
using bz.iteam.crm.web.Models;
using Kendo.Mvc.Extensions;
using Kendo.Mvc.UI;
using Microsoft.AspNet.Identity;
using Microsoft.AspNet.Identity.Owin;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace bz.iteam.crm.web.Controllers
{
#if !Debug
    [RequireHttps]
#endif
    [Authorize]
    public class CallServerController : RepositoryController<CallServer>
    {
        static System.Net.Http.HttpClient HttpClient = new System.Net.Http.HttpClient();

        protected override void WorkspaceLayout()
        {
            ViewBag.Users = AsDataSource<UserController>();
            ViewBag.Franchisees = AsDataSource<FranchiseeController>();

            base.WorkspaceLayout();
        }

        #region .. UpdateChannels ..

        public static bool UpdateChannelsProcessing { get; set; }

        public void UpdateChannels()
        {
            try
            {
#if Debug
              //  return;
#endif

                if (!UpdateChannelsProcessing) try
                    {
                        UpdateChannelsProcessing = true;

                        var callServers = db.CallServers
                            .ToList();

                        foreach (var callServer in callServers)
                        {
                            if (callServer.IsActive && !callServer.Code.ToLower().StartsWith("oktell")) UpdateChannels(callServer);
                            else db.Database.ExecuteSqlCommand($"delete {db.DbName()}.dbo.CallServerDialStates where CallServer_Id = @CallServer_Id", new SqlParameter("@CallServer_Id", callServer.Id));
                        }
                    }
                    finally { UpdateChannelsProcessing = false; }
            }
            catch (Exception ex) { }
        }

        class freeswitch_detailed_call
        {
            public Guid uuid { get; set; }
            public string gateways_name { get; set; }
            public string gsm_name { get; set; }
            public string direction { get; set; }
            public DateTime? created { get; set; }
            public string created_epoch { get; set; }
            public string name { get; set; }
            public string state { get; set; }
            public string cid_name { get; set; }
            public string cid_num { get; set; }
            public string ip_addr { get; set; }
            public string dest { get; set; }
            public string application { get; set; }
            public string application_data { get; set; }
            public string dialplan { get; set; }
            public string context { get; set; }
            public string read_codec { get; set; }
            public string read_rate { get; set; }
            public string read_bit_rate { get; set; }
            public string write_codec { get; set; }
            public string write_rate { get; set; }
            public string write_bit_rate { get; set; }
            public string secure { get; set; }
            public string hostname { get; set; }
            public string presence_id { get; set; }
            public string presence_data { get; set; }
            public string accountcode { get; set; }
            public string callstate { get; set; }
            public string callee_name { get; set; }
            public string callee_num { get; set; }
            public string callee_direction { get; set; }
            public Guid? call_uuid { get; set; }
            public string sent_callee_name { get; set; }
            public string sent_callee_num { get; set; }

            public Guid? b_uuid { get; set; }
            public string b_gateways_name { get; set; }
            public string b_gsm_name { get; set; }
            public string b_direction { get; set; }
            public DateTime? b_created { get; set; }
            public string b_created_epoch { get; set; }
            public string b_name { get; set; }
            public string b_state { get; set; }
            public string b_cid_name { get; set; }
            public string b_cid_num { get; set; }
            public string b_ip_addr { get; set; }
            public string b_dest { get; set; }
            public string b_application { get; set; }
            public string b_application_data { get; set; }
            public string b_dialplan { get; set; }
            public string b_context { get; set; }
            public string b_read_codec { get; set; }
            public string b_read_rate { get; set; }
            public string b_read_bit_rate { get; set; }
            public string b_write_codec { get; set; }
            public string b_write_bit_rate { get; set; }
            public string b_secure { get; set; }
            public string b_hostname { get; set; }
            public string b_presence_id { get; set; }
            public string b_presence_data { get; set; }
            public string b_accountcode { get; set; }
            public string b_callstate { get; set; }
            public string b_callee_name { get; set; }
            public string b_callee_num { get; set; }
            public string b_callee_direction { get; set; }
            public Guid? b_call_uuid { get; set; }
            public string b_sent_callee_name { get; set; }
            public string b_sent_callee_num { get; set; }
            public string call_created_epoch { get; set; }
        }

        class freeswitch_detailed_calls
        {
            public int row_count { get; set; }
            public List<freeswitch_detailed_call> rows { get; set; }
        }

        public /*async*/ void UpdateChannels(CallServer callServer)
        {
            //await Task.Factory.StartNew(() =>
            //{
            try
            {
                ServicePointManager.ServerCertificateValidationCallback += (se, cert, chain, sslerror) => { return true; }; // игнор недействительных сертификатов

                var requestPHP = HttpWebRequest.CreateHttp(new Uri($"{callServer.URL.Replace("https:", "http:")}:9933/commands?command=show detailed_calls as json".Replace("/:9933", ":9933")));
                requestPHP.Method = "GET";
                requestPHP.ContentType = "application/json";

                string json = null;
                HttpWebResponse response = requestPHP.GetResponse() as HttpWebResponse;
                using (Stream responseStream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(responseStream, Encoding.UTF8);
                    json = reader.ReadToEnd();
                }

                var fs_calls = Newtonsoft.Json.JsonConvert.DeserializeObject<freeswitch_detailed_calls>(json);
                var db_calls = db.CallServerDialStates.AsNoTracking().Where(e => e.CallServer_Id == callServer.Id).ToList();
                var lines = GetLines(callServer.Id);

                db_calls.RemoveAll(db_call => !lines.Any(line => line.Gateway == db_call.Gateway && line.Line == db_call.Line));
                db_calls.RemoveAll(db_call => !(fs_calls.rows ?? new List<freeswitch_detailed_call>()).Any(fs_call => fs_call.uuid == db_call.UUID));

                foreach (var line in lines)
                {
                    if (!db_calls.Any(e => e.Gateway == line.Gateway && e.Line == line.Line)) db_calls.Add(new CallServerDialState
                    {
                        CallServer_Id = callServer.Id,
                        Name = line.Name.ToLower().StartsWith("gsm") && line.Name.Split('_').Length >= 2 ? $"{line.Name.Split('_')[0]}_{line.Name.Split('_')[1]}" : line.Name,
                        Gateway = line.Gateway,
                        Line = line.Line
                    });
                }

                const string str_sofia_internal = "sofia/internal/";
                const string str_sofia_external = "sofia/external/";
                const string str_internal = "internal";
                const string str_external = "external";

                foreach (var fs_call in fs_calls.rows ?? new List<freeswitch_detailed_call>())
                {
                    var profile = fs_call.name.StartsWith(str_sofia_internal) ? str_internal : str_external;
                    var sip = profile == str_external;
                    var db_call = db_calls.FirstOrDefault(e => e.UUID == fs_call.uuid);

                    if (db_call == null)
                    {
                        if (sip)
                        {
                            var gateway = fs_call.gateways_name;
                            if ((db_call = db_calls.Where(e => e.Gateway == gateway).OrderBy(e => e.Line).FirstOrDefault(e => e.UUID == null)) == null) continue;
                        }
                        else
                        {
                            //var gateway = fs_call.name.Split('@')[1];
                            try
                            {
                                var gateway = fs_call.gsm_name;
                                var name = fs_call.name.Split('@')[0].Replace(str_sofia_internal, null);
                                var line = int.Parse(char.IsDigit(name.First()) ? name.Left(2) : name.Right(2));
                                if ((db_call = db_calls.FirstOrDefault(e => e.Gateway == gateway && e.Line == line)) == null) continue;
                            }
                            catch (Exception exx)
                            {
                                NLog.Fluent.Log.Error().Message($"{callServer.Name}: failed: {fs_call.name}").Write();
                                throw exx;
                            }
                        }

                        db_call.UUID = fs_call.uuid;
                    }

                    db_call.Created = fs_call.created;
                    db_call.Direction = fs_call.direction;
                    db_call.Profile = profile;
                    db_call.State = fs_call.state;
                    db_call.CallState = fs_call.callstate;
                    db_call.CallerID = (sip ? fs_call.name.Replace(str_sofia_external, null) : fs_call.name.Split('@')[0].Replace(str_sofia_internal, null).Remove(0, 2)).Left(20);
                    db_call.Dest = fs_call.dest == fs_call.callee_num ? sip ? fs_call.dest : fs_call.dest.Remove(0, 2) : fs_call.dest;
                    db_call.ApplicationData = string.IsNullOrEmpty(fs_call.application) ? null : $"{fs_call.application}: {fs_call.application_data}";

                    db_call.B_UUID = fs_call.b_uuid;
                    db_call.B_Created = fs_call.b_created;
                    db_call.B_Direction = fs_call.b_direction;
                    db_call.B_Profile = fs_call.b_name.StartsWith(str_sofia_internal) ? str_internal : str_external;
                    db_call.B_State = fs_call.b_state;
                    db_call.B_CallState = fs_call.b_callstate;
                    db_call.B_Gateway = fs_call.b_gateways_name;
                    db_call.B_Dest = fs_call.b_dest.Right(20);
                }

                var to_delete = string.Join(",", db_calls.Select(e => e.Id).Distinct());
                var to_insert = db_calls.Where(e => e.Id == 0).ToList();
                var to_update = db_calls.Where(e => e.Id != 0).ToList();

                NLog.Fluent.Log.Trace().Message($"{callServer.Name}: begin transaction").Write();
                var tran = db.Database.BeginTransaction();
                try
                {
                    // delete
                    if (!string.IsNullOrEmpty(to_delete)) db.Database.ExecuteSqlCommand($"delete {db.DbName()}.dbo.CallServerDialStates where CallServer_Id = @CallServer_Id and Id not in ({to_delete})", new SqlParameter("@CallServer_Id", callServer.Id));

                    // insert
                    if (to_insert.Count != 0)
                    {
                        foreach (var call in to_insert) db.CallServerDialStates.Add(call);
                        db.SaveChanges();
                    }

                    NLog.Fluent.Log.Trace().Message($"{callServer.Name}: commit transaction").Write();
                    tran.Commit();

                    // update
                    if (to_update.Count != 0)
                    {
                        foreach (var call in to_update) { db.CallServerDialStates.Attach(call); db.Entry(call).State = EntityState.Modified; }
                        db.SaveChanges();
                    }
                }
                catch (Exception exx)
                {
                    NLog.Fluent.Log.Trace().Message($"{callServer.Name}: rollback transaction").Write();
                    tran.Rollback();
                }
            }
            catch (Exception ex)
            {
                NLog.Fluent.Log.Trace().Message($"{callServer.Name}: failed: {ex.Message}").Write();
                NLog.Fluent.Log.Error().Message($"{callServer.Name}: failed").Exception(ex).Write();
            }
            // });
        }

        class freeswitch_Gateway
        {
            public string uuid { get; set; }
            public string extension { get; set; }
            public string password { get; set; }
            public string description { get; set; }
            public string IP { get; set; }
            public long countLines { get; set; }
            public bool isSip { get; set; }
            public string domainUuid { get; set; }
            public string noReg { get; set; }
            public string callerId { get; set; }
            public string prefix { get; set; }
        }

        public class freeswitch_Line
        {
            //public string Password { get; set; }
            //public bool NoReg { get; set; }
            //public string CallerId { get; set; }
            //public string Prefix { get; set; }

            public string Gateway { get; set; }
            public int Line { get; set; }
            public string Name { get; set; }
        }

        public class freeswitch_Registration
        {
            //\"reg_user\":\"gsm27_megafon_05\",
            //\"realm\":\"test2.mdcnt.ru\",
            //\"token\":\"1781982986@81.162.16.47\",
            //\"url\":\"sofia/internal/sip:gsm27_megafon_05@81.162.16.47:5203\",
            //\"expires\":\"1559114159\",
            //\"network_ip\":\"81.162.16.47\",
            //\"network_port\":\"5203\",
            //\"network_proto\":\"udp\",
            //\"hostname\":\"fsDeb9Svr\",
            //\"metadata\":\"\"

            public string reg_user { get; set; }
            public string realm { get; set; }
            public string token { get; set; }
            public string url { get; set; }
            public string expires { get; set; }
            public string network_ip { get; set; }
            public string network_port { get; set; }
            public string network_proto { get; set; }
            public string hostname { get; set; }
            public string metadata { get; set; }
        }

        class freeswitch_Registrations
        {
            public int row_count { get; set; }
            public List<freeswitch_Registration> rows { get; set; }
        }

        public class freeswitch_gsm_registration
        {
            /*
            "Call-ID":"553704364@37.221.200.66"
            "User":"gsm139_rt_24@medest.mdcnt.ru"
            "Agent":"dble"
            "Status":"Registered(UDP)(unknown) EXP(2019-05-30 13:56:54) EXPSECS(289)"
            "Ping-Status":"Reachable"
            "Ping-Time":"0.00"
            "Host":"fsDeb9Svr"
            "IP":"37.221.200.66"
            "Port":"5084"
            "Auth-User":"gsm139_rt_24"
            "Auth-Realm":"medest.mdcnt.ru"
            "MWI-Account":"gsm139_rt_24@medest.mdcnt.ru"
            */
        }

        class freeswitch_gsm_registrations
        {
            public int row_count { get; set; }
            public List<freeswitch_gsm_registration> rows { get; set; }
        }


        List<freeswitch_Line> GetLines(long callServer_Id)
        {
            ServicePointManager.ServerCertificateValidationCallback += (se, cert, chain, sslerror) => { return true; }; // игнор недействительных сертификатов

            var requestPHP = HttpWebRequest.CreateHttp(new Uri($"{db.CallServers.Where(e => e.Id == callServer_Id).FirstOrDefault().URL}/crmApi/ReadExtensionGateway.php"));
            requestPHP.Method = "POST";
            requestPHP.ContentType = "application/json";

            string json = null;
            HttpWebResponse response = requestPHP.GetResponse() as HttpWebResponse;
            using (Stream responseStream = response.GetResponseStream())
            {
                StreamReader reader = new StreamReader(responseStream, Encoding.UTF8);
                json = reader.ReadToEnd();
            }

            List<freeswitch_Gateway> gateways = Newtonsoft.Json.JsonConvert.DeserializeObject<List<freeswitch_Gateway>>(json);

            requestPHP = HttpWebRequest.CreateHttp(new Uri($"{db.CallServers.Where(e => e.Id == callServer_Id).FirstOrDefault().URL}/crmApi/ShowReistrations.php"));
            requestPHP.Method = "POST";
            requestPHP.ContentType = "application/json";

            string[] jsons = null;
            response = requestPHP.GetResponse() as HttpWebResponse;
            using (Stream responseStream = response.GetResponseStream())
            {
                StreamReader reader = new StreamReader(responseStream, Encoding.UTF8);
                jsons = reader.ReadToEnd().Split(new string[] { "||" }, StringSplitOptions.None);
            }

            var gsm_registrations = Newtonsoft.Json.JsonConvert.DeserializeObject<freeswitch_Registrations>(jsons[0]);
            var sip_registrations = jsons[1];

            List<freeswitch_Line> result = new List<freeswitch_Line>();
            for (var i = 0; i < gateways.Count; i++)
            {
                var gsm_registration = gsm_registrations.rows == null ? null : gsm_registrations.rows.FirstOrDefault(e => e.reg_user == gateways[i].extension);
                if (gsm_registration != null)
                {
                    int lineIndex = 0;
                    int.TryParse(gateways[i].extension.Length > 2 ? gateways[i].extension.Substring(gateways[i].extension.Length - 2, 2) : string.Empty, out lineIndex);

                    result.Add(new freeswitch_Line
                    {
                        //Gateway = registration.url.Split(';')[0].Split('@')[1],
                        Gateway = gateways[i].extension,
                        Line = lineIndex,
                        Name = gateways[i].extension,
                    });
                }
                else
                {
                    var sip_registration = (sip_registrations.Split(new[] { gateways[i].extension + "@" + gateways[i].IP + "\t" }, StringSplitOptions.None).Count() > 1 ? sip_registrations.Split(new[] { gateways[i].extension + "@" + gateways[i].IP + "\t" }, StringSplitOptions.None)[1].Split(' ')[0] : null);
                    if (sip_registration != null)
                    {
                        var linesCount = 1;
                        int.TryParse(gateways[i].description.Split('_').LastOrDefault(), out linesCount);

                        for (int index = 1; index <= linesCount; index++)
                        {
                            result.Add(new freeswitch_Line
                            {
                                Gateway = $"{gateways[i].extension}@{gateways[i].IP}",
                                Line = index,
                                Name = gateways[i].extension
                            });
                        }
                    }
                }
            }

            return result;
        }

        #endregion

        #region .. OutCalls ..

        public static bool OutCallsProcessing = false;


        public void OutCalls()
        {
            try
            {
                if (!OutCallsProcessing) try
                    {
                        OutCallsProcessing = true;
                        var now = DateTime.Now;
                        var timeOfDay = DateTime.Now.TimeOfDay;

                        var jobs = db.CallJobs
                            .Where(job => job.IsActive)
                            .Where(job => job.LastIteration < now || job.LastIteration == null)
                            .Where(job => job.TimeStart <= timeOfDay && job.TimeStop > timeOfDay)
                            .Include(job => job.CallProject)
                            .AsNoTracking()
                            .ToList();

                        foreach (var job in jobs)
                        {
                            switch (job.Type)
                            {
                                case CallJobType.OutboundProgressive:
                                case CallJobType.OutboundPreview:

                                    if (job.DailyLimitClients != 0) if (job.DailyLimitClients <= db.Database.SqlQuery<int>($"select count(0) from {db.DbName()}.dbo.Clients where CallJob_Id = @Id and cast(CreationDate as date) = cast(getDate() as date)", new SqlParameter("@Id", job.Id)).First()) continue;

                                    db.Database.ExecuteSqlCommand(
                                        $"update {db.DbName()}.dbo.CallJobs set LastIteration = dateAdd(second, DialInterval, @Now) where Id = @Id",
                                        new SqlParameter("@Id", job.Id),
                                        new SqlParameter("@Now", DateTime.Now));

                                    SwitchScripts(job);

                                    OutCallsProcess(job);

                                    break;

                                default:
                                   
                                    break;
                            }
                        }
                    }
                    finally { OutCallsProcessing = false; }
            }
            catch (Exception ex) 
            {
                NLog.Fluent.Log.Error().Exception(ex).Write();
            }
        }

        public void SwitchScripts(CallJob job)
        {
            try
            {
                var switches = db.Database.SqlQuery<string>($@"
					select	top(1) NumberRatios
					from	{db.DbName()}.dbo.CallJobSwitches
					where	CallJob_Id = @CallJob_Id
					and		DayOfWeek = @DayOfWeek 
					and		datePart(hour, TimeStart) = datePart(hour, getDate())
					and		datePart(minute, TimeStart) = datePart(minute, getDate())
					",
                    new SqlParameter("@CallJob_Id", job.Id),
                    new SqlParameter("@DayOfWeek", (int)DateTime.Now.DayOfWeek)).FirstOrDefault();

                if (switches != null)
                {
                    var ratios = System.Web.Helpers.Json.Decode<List<CallJobScriptRatio>>(switches);
                    foreach(var ratio in ratios)
                    {
                        db.Database.ExecuteSqlCommand($@"
							update	{db.DbName()}.dbo.CallJobScripts
							set		NumberRatio = @NumberRatio
							where	CallJob_Id = @CallJob_Id
							and		Name = @ScriptName
							",
                            new SqlParameter("@CallJob_Id", job.Id),
                            new SqlParameter("@ScriptName", ratio.ScriptName),
                            new SqlParameter("@NumberRatio", ratio.Value));

                        NLog.Fluent.Log.Trace().Message($"{job.Name}: {ratio.ScriptName}: {ratio.Value}").Write();
                    }
                }
            }
            catch (Exception ex)
            {
                NLog.Fluent.Log.Error().Message($"{job.Name}: failed").Exception(ex).Write();
            }
        }

        public async void OutCallsProcess(CallJob job)
        {
            try
            {
                try
                {
                    NLog.Fluent.Log.Trace().Message($"{job.Name}: begin").Write();

                    const int mixScale = 16;
                    const int maxPacket = 100;

                    // адрес сервера
                    var sender = db.Options.FirstOrDefault(x => x.Name == Option.SenderURL)?.Value;
                    if (sender == null) return;

                    // доступные направления дозвона
                    var projectRoutes = db.CallProjectRoutes.Where(route => route.CallProject_Id == job.CallProject_Id).Where(route => route.CallProjectRouteLines.Count() != 0).AsNoTracking().ToList();
                    if (projectRoutes.Count == 0) return;

                    // доступные линии направлений дозвона
                    var routeLines = projectRoutes.Where(route => route.CallProjectRouteLines != null).SelectMany(route => route.CallProjectRouteLines).Select(line => line.CallProjectRoute.Prefix + "_" + line.Provider.Code).Distinct().ToList();
                    if (routeLines.Count == 0) return;

                    // доступные направления на серверах дозвона
                    var readyStates = db.CallServerLineStates.Where(e => e.CallServer.IsActive).Where(state => routeLines.Any(line => state.Prefix + "_" + state.Provider.Code == line)).AsNoTracking().ToList();
                    if (readyStates.Count == 0) return;

                    // доступные линии на серверах дозвона
                    var readyLines = readyStates.Sum(line => line.ReadyCount + line.CallCount / mixScale);
                    if (readyLines == 0) return;

                    // живые направления
                    var readyRoutes = projectRoutes.Where(route => route.CallProjectRouteLines.Select(line => line.CallProjectRoute.Prefix + "_" + line.Provider.Code).Distinct().Any(line => db.CallServerLineStates.Where(e => e.CallServer.IsActive).Where(state => (state.ReadyCount + state.CallCount / mixScale) != 0).Select(state => state.Prefix + "_" + state.Provider.Code).Contains(line))).ToList();

                    var fixProviders = readyRoutes.Where(route => route.Provider_Id != null).Select(route => route.Provider_Id).ToList();
                    var anyProviders = projectRoutes.Where(route => route.Provider_Id != null).Select(route => route.Provider_Id).ToList();

                    // запрос для фильтрации по живым направлениям
                    var fixQuery = fixProviders.Any() ? $"cbn.Provider_Id in ({string.Join(",", fixProviders)})" : "1=2";
                    var anyQuery = readyRoutes.Any(route => route.Provider_Id == null) ? $"cbn.Provider_Id not in ({string.Join(",", anyProviders)})" : "1=2";
                    var queryRoutes = $"({fixQuery} or {anyQuery})";

                    // операторы на задаче
                    var jobOperators = db.OperatorCallJobs.Where(e => e.CallJob_Id == job.Id).Include(e => e.Operator).Select(e => e.Operator).AsNoTracking().ToList();

                    // доступные операторы
                    var readyOperators = jobOperators.Count(e => !e.IsCatch && e.State == OperatorState.Online);
                    if (job.NumberFactorCallState || job.Type == CallJobType.OutboundPreview) readyOperators += jobOperators.Count(e => !e.IsCatch && e.State == OperatorState.Call);

                    var totalNumbers = Math.Ceiling(Math.Min(job.NumberFactor == 0 ? int.MaxValue : readyOperators * job.NumberFactor, readyLines) * job.NumberRatio / 100.0);
                    var testNumbers = Convert.ToInt32(Math.Ceiling(job.CallProject.DialTestPercent * totalNumbers));
                    var callNumbers = Convert.ToInt32(Math.Ceiling((1 - job.CallProject.DialTestPercent) * totalNumbers));

                    var process_Id = Guid.NewGuid();

                    NLog.Fluent.Log.Trace().Message($"{job.Name}: readyOperators: {readyOperators} / numberFactor: {job.NumberFactor} / readyLines: {readyLines} / process_Id: {process_Id}").Write();

                    var query = $@"
						insert  {db.DbName()}.dbo.CallBaseNumbers_process_{job.CallProject_Id} (Id, Provider_Id, Phone, Process_Id)
						select	cbn.Id, cbn.Provider_Id, cbn.Phone, @process_Id
						from	{db.DbName()}.dbo.CallBaseNumbers_{job.CallProject_Id} cbn,
								{db.DbName()}.dbo.CallBases cb
						where	cb.Name = 'ExpressTest'
						and		not exists (select * from {db.DbName()}.dbo.CallBaseNumbers_process_{job.CallProject_Id} p where p.Id = cbn.Id)
						and		(State = 'N' or (State = 'T' and datediff(minute, isnull(LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= {job.CallProject.RecallTime}) or (State = 'L' and datediff(minute, isnull(LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= 5) or (State = 'D' and datediff(minute, isnull(LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= 30))
						and		cbn.CallBase_Id = cb.Id
						and		cb.IsActive = 1
						and		cb.CallProject_Id = @CallProject_Id
						and     {queryRoutes}

						if ((select count(0) from {db.DbName()}.dbo.CallBaseNumbers_process_{job.CallProject_Id} where Process_Id = @process_Id) = 0)
						insert  {db.DbName()}.dbo.CallBaseNumbers_process_{job.CallProject_Id} (Id, Provider_Id, Phone, Process_Id)
						select	top (@callNumbers) cbn.Id, cbn.Provider_Id, cbn.Phone, @process_Id
						from	{db.DbName()}.dbo.CallBaseNumbers_{job.CallProject_Id} cbn,
								{db.DbName()}.dbo.CallBases cb tablesample(1000000 rows)
						where	not exists (select * from {db.DbName()}.dbo.StopListNumbers b where /*right(b.Phone, 10) = right(cbn.Phone, 10)*/ b.Phone = cbn.Phone)
						and		not exists (select * from {db.DbName()}.dbo.CallBaseNumbers_process_{job.CallProject_Id} p where p.Id = cbn.Id)
						and		(State = 'N' or (State = 'T' and datediff(minute, isnull(LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= {job.CallProject.RecallTime}) or (State = 'L' and datediff(minute, isnull(LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= 5) or (State = 'D' and datediff(minute, isnull(LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= 30))
						and		cbn.CallBase_Id = cb.Id
						and		cb.IsActive = 1
						and		cb.CallProject_Id = @CallProject_Id
						and     {queryRoutes}
						--and		(abs(cast((binary_checksum(*) * rand()) as int)) % 100) < 10
						order   by newid()

						select Id, Provider_Id, Phone from {db.DbName()}.dbo.CallBaseNumbers_process_{job.CallProject_Id} where Process_Id = @process_Id
						union all
						select * from (select top (@testNumbers) 0 Id, Provider_Id, Phone  from {db.DbName()}.dbo.CallProjectTestNumbers n join {db.DbName()}.dbo.CallProjectTests t on n.CallProjectTest_Id = t.Id where t.CallProject_Id = @CallProject_Id and State = 'N' order by newid()) cptn
						";

                    var series = db.Database.SqlQuery<CBNumber>(query,
                        new SqlParameter("@callNumbers", callNumbers),
                        new SqlParameter("@testNumbers", testNumbers),
                        new SqlParameter("@CallProject_Id", job.CallProject_Id),
                        new SqlParameter("@process_Id", process_Id))
                        .GroupBy(e => e.Provider_Id)
                        .Select(e =>
                            new
                            {
                                CallBaseNumbers = e.Select(number => number),
                                Routes = projectRoutes.Where(route => route.Provider_Id == e.Key || (route.Provider_Id == null && !projectRoutes.Any(x => x.Provider_Id == e.Key)))
                            })
                        .ToList();

                    var totalReady = db.CallServerLineStates.Where(e => e.CallServer.IsActive).Sum(line => line.ReadyCount + 1.0 * line.CallCount / mixScale);
                    NLog.Fluent.Log.Trace().Message($"{job.Name}: totalReady: {totalReady}").Write();

                    var rates = db.CallServerLineStates.Where(e => e.CallServer.IsActive).Select(line => new
                    {
                        CallServer = line.CallServer,
                        Line = (line.Prefix == null ? "" : line.Prefix + "_") + line.Provider.Code,
                        Rate = totalReady == 0 ? 0.0 : (line.ReadyCount + 1.0 * line.CallCount / mixScale) / totalReady
                    }).AsNoTracking().ToList();

                    NLog.Fluent.Log.Trace().Message($"{job.Name}: rates: {string.Join(" / ", rates.GroupBy(e => e.CallServer.Code).Select(e => $"{e.Key}:{string.Join(":", e.Where(x => x.Rate > 0).Select(x => x.Line)).Nullify() ?? "*"}"))}").Write();

                    // выбор следующего CallerID
                    var routes = projectRoutes.ToDictionary(key => key.Id, value => new { Selector = value.CallerSelector, Callers = value.CallProjectRouteCallers?.ToList() ?? new List<CallProjectRouteCaller>() });
                    var callers = projectRoutes.ToDictionary(key => key.Id, value => new long());
                    var getRouteCaller = new Func<long, string>(id =>
                    {
                        if (routes[id].Callers.Count == 0) return null;
                        var last = routes[id].Callers.IndexOf(routes[id].Callers.FirstOrDefault(x => x.Id == callers[id]));
                        var next = -1;

                        switch (routes[id].Selector)
                        {
                            case CallerSelector.Random:
                                next = new Random(last).Next(routes[id].Callers.Count);
                                break;

                            case CallerSelector.Rotation:
                                if ((next = last + 1) >= routes[id].Callers.Count) next = 0;
                                break;
                        }

                        callers[id] = routes[id].Callers.ElementAtOrDefault(next)?.Id ?? new long();
                        return routes[id].Callers.FirstOrDefault(x => x.Id == callers[id])?.Phone;
                    });

                    var old_number_ids = job.CallJobNumbers.Select(e => e.Id).ToList();
                    var old_cbnumber_ids = job.CallJobNumbers.Select(e => e.CallBaseNumber_Id).Distinct().ToList();

                    foreach (var serie in series)
                    {
                        NLog.Fluent.Log.Trace().Message($"{job.Name}: serie: try").Write();

                        var serieWant = serie.CallBaseNumbers.Count();
                        var serieNumbers = serie.CallBaseNumbers.ToList();

                        NLog.Fluent.Log.Trace().Message($"{job.Name}: serieWant: {serieWant}").Write();
                        NLog.Fluent.Log.Trace().Message($"{job.Name}: serieNumbers: {string.Join(" / ", serieNumbers.Take(5).Select(e => e.Phone))}{(serieWant > 5 ? " / ..." : string.Empty)}").Write();

                        var serieLines = serie.Routes
                            .SelectMany(route => route.CallProjectRouteLines.Select(line => new { Line = (route.Prefix == null ? "" : route.Prefix + "_") + line.Provider.Code, Interval = route.Interval, Route_Id = route.Id }))
                            .Join(rates, line => line.Line, rate => rate.Line, (line, rate) => new { Line = line.Line, Interval = line.Interval, Rate = rate.Rate, Server = rate.CallServer, Route_Id = line.Route_Id })
                            .OrderByDescending(line => line.Rate)
                            .ToList();

                        var serieRate = serieLines.Sum(e => e.Rate);

                        NLog.Fluent.Log.Trace().Message($"{job.Name}: serieRate: {serieRate}").Write();
                        NLog.Fluent.Log.Trace().Message($"{job.Name}: serieLines: {string.Join(" / ", serieLines.Select(e => e.Server.Code + "_" + e.Line))}").Write();

                        foreach (var line in serieLines)
                        {
                            var lineWant = Convert.ToInt32(Math.Ceiling(serieRate == 0 ? 0 : serieWant * (1 / serieRate * line.Rate)));
                            var lineNumbers = serieNumbers.Take(Math.Min(maxPacket, Math.Min(lineWant, serieNumbers.Count))).ToList();
                            var lineNumbersCount = lineNumbers.Count;
                            if (lineNumbersCount == 0) break;

                            serieNumbers.RemoveRange(0, lineNumbers.Count);

                            foreach (var script in db.CallJobScripts.Where(e => e.CallJob_Id == job.Id).Where(e => e.NumberRatio > 0).OrderBy(e => Guid.NewGuid())/*.OrderByDescending(e => e.NumberRatio)*/.AsNoTracking().ToList())
                            {
                                var scriptNumbers = lineNumbers.Take(Convert.ToInt32(Math.Ceiling(Math.Min(lineNumbers.Count, lineNumbersCount * script.NumberRatio / 100.0)))).ToList();
                                if (scriptNumbers.Count == 0) break;

                                lineNumbers.RemoveRange(0, scriptNumbers.Count);

                                var packet = new CallBasePacket
                                {
                                    Sender = sender,
                                    CallProject_Id = job.CallProject_Id,
                                    CallJob_Id = job.Id,
                                    ScriptName = script?.Name,
                                    DialTime = job.DialTime,
                                    Interval = line.Interval,
                                    Lines = new List<CBPacketLine> { new CBPacketLine() { Line = line.Line } },
                                    Numbers = new List<CBPacketNumber>(scriptNumbers.Select(number => new CBPacketNumber { Id = number.Id, CallerID = getRouteCaller(line.Route_Id), Phone = number.Phone }))
                                };

                                var packetJson = Encode(packet);

                                if (packet.Numbers.Count() != 0) db.Database.ExecuteSqlCommand($@"
								    update	{db.DbName()}.dbo.CallBaseNumbers_{job.CallProject_Id}
								    set     State = 'P',
										    LastAttemptDate = getDate()
								    where   Id in ({string.Join(",", packet.Numbers.Select(e => e.Id))})
								    ");

                                if (packet.Numbers.Where(e => e.Id == 0).Count() != 0) db.Database.ExecuteSqlCommand($@"
								    update  {db.DbName()}.dbo.CallProjectTestNumbers
								    set     State = 'P',
                                            LastAttemptDate = getDate()
								    where   Phone in ({string.Join(",", packet.Numbers.Where(e => e.Id == 0).Select(e => "'" + e.Phone + "'"))})
								    ");

                                switch (job.Type)
                                {
                                    case CallJobType.OutboundProgressive:   // формируем запрос на сервер...
                                        
                                        ServicePointManager.ServerCertificateValidationCallback += (se, cert, chain, sslerror) => { return true; }; // игнор недействительных сертификатов

                                        if (line.Server.Code.ToLower().Contains("oktell"))
                                        {
                                            await HttpClient.PostAsync(new Uri($"{line.Server.URL}/crm/ProgressiveAutoCalls_post.svc"), new System.Net.Http.StringContent(packetJson, Encoding.UTF8, "application/json"));
                                        }
                                        else
                                        {
                                            var packetId = Guid.NewGuid().ToString().Substring(0, 8);
                                            var packetDt = DateTime.Now;
                                            //NLog.Fluent.Log.Trace().Message($"{job.Name}: packet: {line.Server.URL} {packet.Numbers} send: {packetId} / {line.Line} / {packet.Numbers.Count}").Write();

                                            StringBuilder builderLines = new StringBuilder();
                                            foreach (var lin in packet.Lines)
                                            {
                                                builderLines.Append(lin.Line).Append(",");
                                            }
                                            string lines = builderLines.ToString();

                                            StringBuilder builderNums = new StringBuilder();
                                            foreach (var cbn in scriptNumbers)
                                            {
                                                builderNums.Append(cbn.Phone).Append(",");
                                            }
                                            string numbers = builderNums.ToString();

                                            //if (line.Server.URL == "https://10.100.8.101")
                                            //{
                                                NLog.Fluent.Log.Trace().Message($"{job.Name}: попытка отправки packet:  {packetId}: {line.Server.URL}").Write();
                                                try
                                                {
                                                    HttpClient httpClient = new HttpClient();
                                                    string url = line.Server.URL.Replace("https", "http");
                                                    if (url.EndsWith("/"))
                                                        url = url.Remove(url.Length - 1);
                                                    httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                                                    var response = httpClient.PostAsync(new Uri($"{url}:9933/call_base_insert"), new StringContent(packetJson, Encoding.UTF8, "application/json")).Result;
                                                    //response.PostAsync(new Uri($"{line.Server.URL}/ProgressiveAutoCalls_post.php"), new StringContent(packetJson, Encoding.UTF8, "application/json"))
                                                    string responseBody = await response.Content.ReadAsStringAsync();
                                                    var responseDict = JsonConvert.DeserializeObject<Dictionary<string, string>>(responseBody);
                                                    if (responseDict.ContainsValue("ok"))
                                                    {
                                                        NLog.Fluent.Log.Trace().Message($"{job.Name}: Отправлено!!! packet: {line.Server.URL} {lines} {numbers} done: {packetId}: {(DateTime.Now - packetDt).TotalMilliseconds} ms response: {responseBody}").Write();
                                                    }
                                                    else
                                                    {
                                                        if (packet.Numbers.Count() != 0) db.Database.ExecuteSqlCommand($@"
							                                update	{db.DbName()}.dbo.CallBaseNumbers_{job.CallProject_Id}
							                                set     State = 'L'
							                                where   Id in ({string.Join(",", packet.Numbers.Select(e => e.Id))})
							                                ");

                                                        if (packet.Numbers.Where(e => e.Id == 0).Count() != 0) db.Database.ExecuteSqlCommand($@"
							                                update  {db.DbName()}.dbo.CallProjectTestNumbers
							                                set     State = 'N'
							                                where   Phone in ({string.Join(",", packet.Numbers.Where(e => e.Id == 0).Select(e => "'" + e.Phone + "'"))})
							                                ");

                                                        NLog.Fluent.Log.Trace().Message($"{job.Name}: Ошибка!!! packet: {line.Server.URL} {lines} {numbers} done: {packetId}: {(DateTime.Now - packetDt).TotalMilliseconds} ms response: {responseBody}").Write();
                                                    }
                                                    
                                                }
                                                catch (Exception ex)
                                                {
                                                    NLog.Fluent.Log.Trace().Message($"{job.Name}: packet: fail: {packetId}: {line.Server.URL}: {ex.Message}").Write();
                                                }
                                            //}
                                            //else
                                            //{
                                            //    var request = HttpWebRequest.CreateHttp(new Uri($"{line.Server.URL}/ProgressiveAutoCalls_post.php"));
                                            //    request.Method = "POST";
                                            //    request.ContentType = "application/json";

                                            //    using (var streamWriter = new StreamWriter(request.GetRequestStream()))
                                            //    {
                                            //        streamWriter.Write(packetJson);
                                            //        streamWriter.Flush();
                                            //        streamWriter.Close();
                                            //    }

                                            //    await Task.Factory.StartNew(() =>
                                            //    {
                                            //        try
                                            //        {
                                            //            var response = request.GetResponse();

                                            //            NLog.Fluent.Log.Trace().Message($"{job.Name}: Отправлено!!! packet: {line.Server.URL} {lines} {numbers} done: {packetId}: {(DateTime.Now - packetDt).TotalMilliseconds} ms response: {response.Headers.ToString()}").Write();
                                            //        }
                                            //        catch (Exception exx)
                                            //        {
                                            //            NLog.Fluent.Log.Trace().Message($"{job.Name}: packet: fail: {packetId}: {line.Server.URL}: {exx.Message}").Write();
                                            //        }
                                            //    });
                                            //}

                                        }
                                        break;

                                    case CallJobType.OutboundPreview:   // обновляем номера задачи в CallJobNumbers...

                                        db.CallJobNumbers.AddRange(packet.Numbers.Select(number => new CallJobNumber
                                        {
                                            Uuid = Guid.NewGuid(),
                                            CallEffort_Id = null,
                                            Gate = null,
                                            CallJob_Id = packet.CallJob_Id,
                                            CallProject_Id = packet.CallProject_Id,
                                            CallServer_Id = line.Server.Id,
                                            Sender = packet.Sender,
                                            ScriptName = packet.ScriptName,
                                            DialTime = packet.DialTime,
                                            Line = packet.Lines.First().Line,
                                            CallBaseNumber_Id = number.Id,
                                            CallerID = number.CallerID,
                                            Phone = number.Phone,
                                            State = "N"
                                        }));

                                        db.SaveChanges();

                                        break;
                                }
                            } // foreach script
                        } // foreach line

                        NLog.Fluent.Log.Trace().Message($"{job.Name}: serie: done").Write();
                    } // foreach serie

                    NLog.Fluent.Log.Trace().Message($"{job.Name}: GC").Write();
                    {
                        OutCalls_GC(job, process_Id);

                        if (old_number_ids.Count() != 0) db.Database.ExecuteSqlCommand($@"
							delete	{db.DbName()}.dbo.CallJobNumbers
							where   Id in ({string.Join(",", old_number_ids)})
							and		State = 'N'
							");

                        if (old_cbnumber_ids.Count() != 0) db.Database.ExecuteSqlCommand($@"
							update	{db.DbName()}.dbo.CallBaseNumbers_{job.CallProject_Id}
							set     State = 'N'
							where   Id in ({string.Join(",", old_cbnumber_ids)})
							");

                        db.Database.ExecuteSqlCommand($@"
							update	{db.DbName()}.dbo.CallBaseNumbers_{job.CallProject_Id}
							set     State = 'N'
							where   State = 'P' and 
                                    datediff(minute, LastAttemptDate, getdate()) >= 20 and
                                    AttemptsQty is null
							");
                    }

                }
                finally
                {
                    NLog.Fluent.Log.Trace().Message($"{job.Name}: finally").Write();
                }

                NLog.Fluent.Log.Trace().Message($"{job.Name}: success").Write();
            }
            catch (Exception ex)
            {
                NLog.Fluent.Log.Trace().Message($"{job.Name}: failed: {ex.Message}").Write();
                NLog.Fluent.Log.Error().Message($"{job.Name}: failed").Exception(ex).Write();
            }
        }

        public void OutCalls_GC(CallJob job, Guid process_Id)
        {
            try
            {
                db.Database.ExecuteSqlCommand($"delete {db.DbName()}.dbo.CallBaseNumbers_process_{job.CallProject_Id} where Process_Id != @Process_Id", new SqlParameter("@Process_Id", process_Id));
                db.Database.ExecuteSqlCommand($"delete {db.DbName()}.dbo.CallBases where Name = 'ExpressTest' and Counter = Capacity");
            }
            catch (Exception ex) 
            {
                NLog.Fluent.Log.Error().Message($"{job.Name}: failed").Exception(ex).Write();
            }
        }

        #endregion

        #region .. OutChecks ..

        const int CheckProject_DialInterval = 3;

        public static bool OutChecksProcessing { get; set; }

        public void OutChecks()
        {
            try
            {
                if (!OutChecksProcessing) try
                    {
                        OutChecksProcessing = true;

                        var checkProjects = db.CheckProjects
                            .Where(e => e.IsActive)
                            .Where(e => e.LastIteration < DateTime.Now || !e.LastIteration.HasValue)
                            .Where(e => e.TimeStart <= DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second) && e.TimeStop > DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second))
                            //.Where(e => e.CallServersDial)
                            .ToList();

                        foreach (var checkProject in checkProjects)
                        {
                            db.Database.ExecuteSqlCommand($"update {db.DbName()}.dbo.CheckProjects set LastIteration = dateAdd(second, @DialInterval, getDate()) where Id = @Id", new SqlParameter("@Id", checkProject.Id), new SqlParameter("@DialInterval", CheckProject_DialInterval));
                            OutChecksProcess(checkProject);
                        }
                    }
                    finally { OutChecksProcessing = false; }
            }
            catch (Exception ex) { }
        }

        public async void OutChecksProcess(CheckProject checkProject)
        {
            try
            {
                // автоактивация
                db.Database.ExecuteSqlCommand($@"
					if not exists (select * from {db.DbName()}.dbo.CheckBases cb where cb.CheckProject_Id = @CheckProject_Id and cb.IsActive = 1 and cb.HasNumbers = 1)
					begin
						update  {db.DbName()}.dbo.CheckBases
						set     IsActive = 1
						where   Id = (
							select  top 1 cb.Id
							from    SelenaCRM.dbo.CheckBases cb,
									SelenaCRM.dbo.CheckProjects cp
							where   cp.Id = @CheckProject_Id
							and     cp.CheckBasesAutoActivation = 1
							and     cb.CheckProject_Id = @CheckProject_Id
							and     cb.IsActive = 0
							and     cb.HasNumbers = 1
							order   by cb.LoadDate
						)
					end
					", new SqlParameter("@CheckProject_Id", checkProject.Id));

                const int mixScale = 8;
                const int maxPacket = 100;

                var sender = db.Options.First(x => x.Name == Option.SenderURL).Value;
                var routeLines = checkProject.CheckProjectRoutes.SelectMany(route => route.CheckProjectRouteLines.Select(line => route.Prefix + "_" + line.Provider.Code));
                var readyLines = db.CallServerLineStates.Where(e => e.CallServer.IsActive).Where(state => routeLines.Any(line => state.Prefix + "_" + state.Provider.Code == line)).Sum(e => e.CallCount / 2 + e.ReadyCount);
                var process_Id = Guid.NewGuid();

                NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: readyLines: {readyLines}").Write();
                NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: process_Id: {process_Id}").Write();

                var series = db.Database.SqlQuery<CBNumber>($@"
					insert  {db.DbName()}.dbo.CheckBaseNumbers_process_{checkProject.Id} (Id, Provider_Id, Phone, Process_Id)
					select	cbn.Id, cbn.Provider_Id, cbn.Number, @process_Id
					from	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject.Id} cbn,
							{db.DbName()}.dbo.CheckBases cb
					where	cb.Name = 'ExpressTest'
					and		not exists (select * from {db.DbName()}.dbo.CheckBaseNumbers_process_{checkProject.Id} p where p.Id = cbn.Id)
					and		cbn.Status is null
					and		cbn.CheckBase_Id = cb.Id
					and		cb.IsActive = 1 and cb.CheckProject_Id = @CheckProject_Id

					if ((select count(0) from {db.DbName()}.dbo.CheckBaseNumbers_process_{checkProject.Id} where Process_Id = @process_Id) = 0)
					insert  {db.DbName()}.dbo.CheckBaseNumbers_process_{checkProject.Id} (Id, Provider_Id, Phone, Process_Id)
					select	top (@top) cbn.Id, cbn.Provider_Id, cbn.Number, @process_Id
					from	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject.Id} cbn,
							{db.DbName()}.dbo.CheckBases cb tablesample(100000 rows)
					where	not exists (select * from {db.DbName()}.dbo.CheckBaseNumbers_process_{checkProject.Id} p where p.Id = cbn.Id)
					and		cbn.Status is null
					and		cbn.CheckBase_Id = cb.Id
					and		cb.IsActive = 1 and cb.CheckProject_Id = @CheckProject_Id
					--and		(abs(cast((binary_checksum(*) * rand()) as int)) % 100) < 10
					order   by newid()

					select Id, Provider_Id, Phone from {db.DbName()}.dbo.CheckBaseNumbers_process_{checkProject.Id} where Process_Id = @process_Id
					",
                    new SqlParameter("@top", readyLines),
                    new SqlParameter("@CheckProject_Id", checkProject.Id),
                    new SqlParameter("@process_Id", process_Id))
                    .GroupBy(e => e.Provider_Id)
                    .Select(e =>
                        new
                        {
                            CheckBaseNumbers = e.Select(number => number),
                            Routes = checkProject.CheckProjectRoutes.Where(route => route.Provider_Id == e.Key || (route.Provider_Id == null && !checkProject.CheckProjectRoutes.Any(x => x.Provider_Id == e.Key)))
                        });

                var totalReady = db.CallServerLineStates.Where(e => e.CallServer.IsActive).Sum(line => line.ReadyCount + 1.0 * line.CallCount / mixScale);
                NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: totalReady: {totalReady}").Write();

                var rates = db.CallServers.Where(e => e.IsActive).ToList().SelectMany(server => server.CallServerLineStates.ToList().Select(line => new CSRate
                {
                    CallServer = server,
                    Line = (line.Prefix == null ? "" : line.Prefix + "_") + line.Provider.Code,
                    Rate = totalReady == 0 ? 0.0 : (line.ReadyCount + 1.0 * line.CallCount / mixScale) / totalReady
                })).ToList();

                NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: rates: {string.Join(" / ", rates.GroupBy(e => e.CallServer.Code).Select(e => $"{e.Key}:{string.Join(":", e.Where(x => x.Rate > 0).Select(x => x.Line)).Nullify() ?? "*"}"))}").Write();

                // выбор следующего CallerID
                var routes = checkProject.CheckProjectRoutes.ToDictionary(key => key.Id, value => new { Selector = value.CallerSelector, Callers = value.CheckProjectRouteCallers.ToList() });
                var callers = checkProject.CheckProjectRoutes.ToDictionary(key => key.Id, value => new long());
                var getRouteCaller = new Func<long, string>(id =>
                {
                    if (routes[id].Callers.Count == 0) return null;
                    var last = routes[id].Callers.IndexOf(routes[id].Callers.FirstOrDefault(x => x.Id == callers[id]));
                    var next = -1;

                    switch (routes[id].Selector)
                    {
                        case CallerSelector.Random:
                            next = new Random(last).Next(routes[id].Callers.Count);
                            break;

                        case CallerSelector.Rotation:
                            if ((next = last + 1) >= routes[id].Callers.Count) next = 0;
                            break;
                    }

                    callers[id] = routes[id].Callers.ElementAtOrDefault(next)?.Id ?? new long();
                    return routes[id].Callers.FirstOrDefault(x => x.Id == callers[id])?.Phone;
                });


                foreach (var serie in series)
                {
                    NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: serie: try").Write();

                    var serieWant = serie.CheckBaseNumbers.Count();
                    var serieNumbers = serie.CheckBaseNumbers.ToList();

                    NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: serieWant: {serieWant}").Write();
                    NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: serieNumbers: {string.Join(" / ", serieNumbers.Take(5).Select(e => e.Phone))}{(serieWant > 5 ? " / ..." : string.Empty)}").Write();

                    var serieLines = serie.Routes
                        .SelectMany(route => route.CheckProjectRouteLines.Select(line => new { Line = (route.Prefix == null ? "" : route.Prefix + "_") + line.Provider.Code, Interval = route.Interval, Route_Id = route.Id }))
                        .Join(rates, line => line.Line, rate => rate.Line, (line, rate) => new { Line = line.Line, Interval = line.Interval, Rate = rate.Rate, Server = rate.CallServer, Route_Id = line.Route_Id })
                        .OrderByDescending(line => line.Rate)
                        .ToList();

                    var serieRate = serieLines.Sum(e => e.Rate);

                    NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: serieRate: {serieRate}").Write();
                    NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: serieLines: {string.Join(" / ", serieLines.Select(e => e.Server.Code + "_" + e.Line))}").Write();

                    foreach (var line in serieLines)
                    {
                        var lineWant = Math.Max(Convert.ToInt32(Math.Ceiling(serieRate == 0 ? 0 : serieWant * (1.0 / serieRate * line.Rate))), 1);
                        var lineNumbers = serieNumbers.Take(Math.Min(maxPacket, Math.Min(lineWant, serieNumbers.Count()))).ToList();

                        if (lineNumbers.Count() == 0) break;

                        serieNumbers.RemoveRange(0, lineNumbers.Count());

                        var packet = new CheckBasePacket
                        {
                            Sender = sender,
                            CheckProject_Id = checkProject.Id,
                            Interval = line.Interval,
                            WithIVR = checkProject.WithIVR ? 1 : 0,
                            Lines = new List<CBPacketLine> { new CBPacketLine() { Line = line.Line } },
                            Numbers = new List<CBPacketNumber>(lineNumbers.Select(number => new CBPacketNumber { Id = number.Id, CallerID = getRouteCaller(line.Route_Id), Phone = number.Phone }))
                        };

                        var packetJson = Encode(packet);

                        // формируем запрос на сервер...
                        ServicePointManager.ServerCertificateValidationCallback += (se, cert, chain, sslerror) => { return true; }; // игнор недействительных сертификатов

                        //#if !oktell
                        //var request = HttpWebRequest.CreateHttp(new Uri($"{line.Server.URL}/crm/CheckCalls_post.svc"));
                        //request.Method = "POST";
                        //request.ContentType = "application/json";

                        //using (var streamWriter = new StreamWriter(request.GetRequestStream()))
                        //{
                        //    streamWriter.Write(packetJson);
                        //    streamWriter.Flush();
                        //    streamWriter.Close();
                        //}
                        //Task.Factory.StartNew(() => request.GetResponse().Close());


                        if (line.Server.Code.ToLower().Contains("oktell"))
                        {
                            var request = HttpWebRequest.CreateHttp(new Uri($"{line.Server.URL}/crm/CheckCalls_post.svc"));
                            request.Method = "POST";
                            request.ContentType = "application/json";

                            using (var streamWriter = new StreamWriter(request.GetRequestStream()))
                            {
                                streamWriter.Write(packetJson);
                                streamWriter.Flush();
                                streamWriter.Close();
                            }

                            var packetId = Guid.NewGuid().ToString().Substring(0, 8);
                            var packetDt = DateTime.Now;
                            NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: packet: send: {packetId} / {line.Line} / {packet.Numbers.Count}").Write();

                            await Task.Factory.StartNew(() =>
                             {
                                 try
                                 {
                                     request.GetResponse();
                                     NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: packet: done: {packetId}: {(DateTime.Now - packetDt).TotalMilliseconds} ms").Write();
                                 }
                                 catch (Exception exx)
                                 {
                                     NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: packet: fail: {packetId}: {line.Server.URL}: {exx.Message}").Write();
                                 }
                             });
                        }
                        else
                        {
                            var packetId = Guid.NewGuid().ToString().Substring(0, 8);
                            var packetDt = DateTime.Now;
                            try
                            {
                                HttpClient httpClient = new HttpClient();
                                string url = line.Server.URL.Replace("https", "http");
                                if (url.EndsWith("/"))
                                    url = url.Remove(url.Length - 1);
                                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                                var response = httpClient.PostAsync(new Uri($"{url}:9933/check_base_insert"), new StringContent(packetJson, Encoding.UTF8, "application/json")).Result;
                                //response.PostAsync(new Uri($"{line.Server.URL}/ProgressiveAutoCalls_post.php"), new StringContent(packetJson, Encoding.UTF8, "application/json"))
                                string responseBody = await response.Content.ReadAsStringAsync();
                                var responseDict = JsonConvert.DeserializeObject<Dictionary<string, string>>(responseBody);
                                if (responseDict.ContainsValue("ok"))
                                {
                                    NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: packet: done: {packetId}: {(DateTime.Now - packetDt).TotalMilliseconds} ms response: {responseBody}").Write();
                                }
                                else
                                {
                                    NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: Ошибка!!! packet: {line.Server.URL} done: {packetId}: {(DateTime.Now - packetDt).TotalMilliseconds} ms response: {responseBody}").Write();
                                }

                            }
                            catch (Exception ex)
                            {
                                NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: packet: fail: {packetId}: {line.Server.URL}: {ex.Message}").Write();
                            }
                            //var request = HttpWebRequest.CreateHttp(new Uri($"{line.Server.URL}/CheckCalls_post.php"));
                            //request.Method = "POST";
                            //request.ContentType = "application/json";

                            //using (var streamWriter = new StreamWriter(request.GetRequestStream()))
                            //{
                            //    streamWriter.Write(packetJson);
                            //    streamWriter.Flush();
                            //    streamWriter.Close();
                            //}

                            //var packetId = Guid.NewGuid().ToString().Substring(0, 8);
                            //var packetDt = DateTime.Now;
                            //NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: packet: send: {packetId} / {line.Line} / {packet.Numbers.Count}").Write();

                            //Task.Factory.StartNew(() =>
                            //{
                            //    try
                            //    {
                            //        request.GetResponse();
                            //        NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: packet: done: {packetId}: {(DateTime.Now - packetDt).TotalMilliseconds} ms").Write();
                            //    }
                            //    catch (Exception exx)
                            //    {
                            //        NLog.Fluent.Log.Trace().Message($"{checkProject.Name}: packet: fail: {packetId}: {line.Server.URL}: {exx.Message}").Write();
                            //    }
                            //});
                        }

                        //#else
                        //                            HttpClient.PostAsync(new Uri($"{line.Server.URL}/crm/CheckCalls_post.svc"), new System.Net.Http.StringContent(packetJson, Encoding.UTF8, "application/json"));
                        //#endif
                    }
                }

                CheckCallWorker_GC(checkProject, process_Id);

                //db.Database.ExecuteSqlCommand($"update {db.DbName()}.dbo.CheckProjects set LastIteration = dateAdd(second, @DialInterval, getDate()) where Id = @Id", new SqlParameter("@Id", checkProject.Id), new SqlParameter("@DialInterval", CheckProjectIteration));
            }
            catch (Exception ex) { }
        }

        public void CheckCallWorker_GC(CheckProject checkProject, Guid process_Id)
        {
            try
            {
                db.Database.ExecuteSqlCommand($"delete {db.DbName()}.dbo.CheckBaseNumbers_process_{checkProject.Id} where Process_Id != @Process_Id", new SqlParameter("@Process_Id", process_Id));
                //db.Database.ExecuteSqlCommand($"delete {db.DbName()}.dbo.CheckBases where Name = 'ExpressTest' and Counter = Capacity");

                db.Database.ExecuteSqlCommand($@"
					update	{db.DbName()}.dbo.CheckBases 
					set		HasNumbers = 0,
							IsActive = 0
					where	Id in
							(
								select	cb.Id
								from	{db.DbName()}.dbo.CheckBases cb
								where	cb.IsActive = 1
								and		cb.HasNumbers = 1
								and		cb.CheckProject_Id = {checkProject.Id}
								and		0 = (select count(0) from {db.DbName()}.dbo.CheckBaseNumbers_{checkProject.Id} where CheckBase_Id = cb.Id and Status is null)
							)
					");
            }
            catch (Exception ex) { }
        }

        #endregion

        #region .. StopListChecker ..

        public static bool StopListCheckerProcessing { get; set; }

        public void StopListChecker()
        {
            try
            {
                if (!StopListCheckerProcessing) try
                    {
                        StopListCheckerProcessing = true;

                        var callProjects = db.CallJobs
                            .Where(e => e.IsActive)
                            .Where(e => e.TimeStart <= DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second) && e.TimeStop > DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second))
                            .Select(e => e.CallProject)
                            .Distinct()
                            .ToList();

                        foreach (var callProject in callProjects)
                        {
                            StopListChecker(callProject);
                        }
                    }
                    finally { StopListCheckerProcessing = false; }
            }
            catch (Exception ex) { }
        }

        public void StopListChecker(CallProject callProject)
        {
            try
            {
                if (callProject.StopListChecker)
                {
                    db.Database.ExecuteSqlCommand($@"
						update	{db.DbName()}.dbo.CallBaseNumbers_{callProject.Id}
						set		State = 'C',
								Descr = '99'
						where	Id in
						(
							select	top (1000) cbn.Id
							from	{db.DbName()}.dbo.CallBaseNumbers_{callProject.Id} cbn
							join	{db.DbName()}.dbo.CallBases cb
							on		cbn.CallBase_Id = cb.Id
							join	{db.DbName()}.dbo.StopListNumbers b
							on		right(b.Phone, 10) = right(cbn.Phone, 10)
							where	cbn.State <> 'C'
							--and		cb.IsActive = 1
							and     cb.CallProject_Id = @CallProject_Id
							order   by cbn.Id
						)",
                        new SqlParameter("@CallProject_Id", callProject.Id));
                }

                if (callProject.CallRouteChecker)
                {
                    // доступные направления дозвона
                    var projectRoutes = db.CallProjectRoutes.Where(route => route.CallProject_Id == callProject.Id).Where(route => route.CallProjectRouteLines.Count() != 0).AsNoTracking().ToList();
                    if (projectRoutes.Count == 0) return;

                    // живые направления
                    var readyRoutes = projectRoutes.Where(route => route.CallProjectRouteLines.Select(line => line.CallProjectRoute.Prefix + "_" + line.Provider.Code).Distinct().Any(line => db.CallServerLineStates.Where(e => e.CallServer.IsActive).Where(state => (state.ReadyCount + state.CallCount) != 0).Select(state => state.Prefix + "_" + state.Provider.Code).Contains(line))).ToList();

                    var fixProviders = readyRoutes.Where(route => route.Provider_Id != null).Select(route => route.Provider_Id).ToList();
                    var anyProviders = projectRoutes.Where(route => route.Provider_Id != null).Select(route => route.Provider_Id).ToList();

                    // запрос для фильтрации по живым направлениям
                    var fixQuery = fixProviders.Any() ? $"Provider_Id in ({string.Join(",", fixProviders)})" : "1=2";
                    var anyQuery = readyRoutes.Any(route => route.Provider_Id == null) ? $"Provider_Id not in ({string.Join(",", anyProviders)})" : "1=2";
                    var queryRoutes = $"({fixQuery} or {anyQuery})";

                    var query = $@"
						update	top(1000) {db.DbName()}.dbo.CallBaseNumbers_{callProject.Id}
						set		State = 'D',
								LastAttemptDate = getDate()
						where	not {queryRoutes}
						and		(State = 'N' or (State = 'D' and datediff(minute, isnull(LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= 30))
						";

                    db.Database.ExecuteSqlCommand(query, new SqlParameter("@CallProject_Id", callProject.Id));
                }
            }
            catch (Exception ex) { }
        }

        #endregion

        #region ..MissedCallsControl..

        public static bool MissedCallsControlProcessing { get; set; }

        public void MissedCallsControll()
        {
            try
            {
                if (!MissedCallsControlProcessing) try
                    {
                        MissedCallsControlProcessing = true;

                        DateTime timeStop = DateTime.Now;
                        DateTime timeStart = timeStop.AddMinutes(-15);



                        var callJobs = db.CallJobs
                            .Where(e => e.IsActive)
                            .Where(e => e.TimeStart <= DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second) && e.TimeStop > DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second))
                            .Where(e => e.IsCreateBaseFromMissedCalls)
                            .Where(e => e.CallProjectIdForMissedCalls > 0)
                            .ToList();

                        foreach (var job in callJobs)
                        {
                            CreateBaseFromMissedCalls(timeStart, timeStop, job);
                            CreateBaseFromShortCalls(timeStart, timeStop, job);
                            //        var name = $"mc {timeStart.ToShortDateString()} / {timeStart.ToShortTimeString()}-{timeStop.ToShortTimeString()} / ({job.Name})";
                            //        var description = "Missed calls auto creation base";

                            //        var numbers = db.Database.SqlQuery<string>($@"
                            //         SELECT CallerId 
                            //            FROM {storage.DbName()}.[dbo].[CallConnections] cc,
                            //            (Select f.TimeOffset
                            //            from
                            //            {db.DbName()}.dbo.CallProjects p,
                            //            {db.DbName()}.dbo.Franchisees f,
                            //            {db.DbName()}.dbo.CallJobs cj
                            //            where cj.Id = @CallJob_Id and  p.Id = cj.CallProject_Id and f.Id = p.Franchisee_Id) f
                            //            where 
                            //            TimeTransfer is not null
                            //            and TimeJoin is null
                            //            and CallJob_Id = @CallJob_Id
                            //            and TimeStart between @DateStart and @DateStop
                            //",
                            //                new SqlParameter("@CallJob_Id", job.Id),
                            //                new SqlParameter("@DateStart", timeStart),
                            //                new SqlParameter("@DateStop", timeStop)).ToList();

                            //        if (numbers.Count == 0) return;

                            //        await callBaseController.CreateNewBase(job.CallProject_Id, job.Id, numbers, timeStart, timeStop, name, description, true);
                        }
                    }
                    finally { MissedCallsControlProcessing = false; }
            }
            catch (Exception ex)
            {
                NLog.Fluent.Log.Error().Exception(ex).Write();
            }
        }

        public async void CreateBaseFromMissedCalls(DateTime timeStart, DateTime timeStop, CallJob job)
        {
            var callBaseController = new CallBaseController();

            var name = $"mc {timeStart.ToShortDateString()} / {timeStart.ToShortTimeString()}-{timeStop.ToShortTimeString()} / ({job.Name})";
            var description = "Missed calls auto creation base";

            var numbers = db.Database.SqlQuery<string>($@"
	                            SELECT CallerId 
                                FROM {storage.DbName()}.[dbo].[CallConnections] cc,
                                (Select f.TimeOffset
                                from
                                {db.DbName()}.dbo.CallProjects p,
                                {db.DbName()}.dbo.Franchisees f,
                                {db.DbName()}.dbo.CallJobs cj
                                where cj.Id = @CallJob_Id and  p.Id = cj.CallProject_Id and f.Id = p.Franchisee_Id) f
                                where 
                                TimeTransfer is not null
                                and TimeJoin is null
                                and CallJob_Id = @CallJob_Id
                                and TimeStart between @DateStart and @DateStop
				                ",
                    new SqlParameter("@CallJob_Id", job.Id),
                    new SqlParameter("@DateStart", timeStart),
                    new SqlParameter("@DateStop", timeStop)).ToList();

            if (numbers.Count == 0) return;

            await callBaseController.CreateNewBase(job.CallProjectIdForMissedCalls, job.Id, numbers, timeStart, timeStop, name, description, true);
        }

        public async void CreateBaseFromShortCalls(DateTime timeStart, DateTime timeStop, CallJob job)
        {
            var callBaseController = new CallBaseController();

            var name = $"sc {timeStart.ToShortDateString()} / {timeStart.ToShortTimeString()}-{timeStop.ToShortTimeString()} / ({job.Name})";
            var description = "Short calls auto creation base";

            var numbers = db.Database.SqlQuery<string>($@"
	                            SELECT oc.CallBaseNumber
                FROM {db.DbName()}.[dbo].[OperatorCalls] oc,
                (Select f.TimeOffset
                 from
                {db.DbName()}.dbo.CallProjects p,
                {db.DbName()}.dbo.Franchisees f,
                {db.DbName()}.dbo.CallJobs cj
                where cj.Id = @CallJob_Id and  p.Id = cj.CallProject_Id and f.Id = p.Franchisee_Id) f
                where Duration > 0 and Duration <= 5 
                and CallJob_Id = @CallJob_Id
                and oc.[TimeStart] between @DateStart and @DateStop
				",
                    new SqlParameter("@CallJob_Id", job.Id),
                    new SqlParameter("@DateStart", timeStart),
                    new SqlParameter("@DateStop", timeStop)).ToList();

            if (numbers.Count == 0) return;

            var result = await callBaseController.CreateNewBase(job.CallProjectIdForMissedCalls, job.Id, numbers, timeStart, timeStop, name, description, true);
            NLog.Fluent.Log.Trace(result.Content);
        }


        #endregion

        #region .. CallNumbersControlByMinuteCalls ..

        public static bool CallNumbersControlByMinuteCallsProcessing { get; set; }

        public void CallNumbersControlByMinuteCalls()
        {
            NLog.Fluent.Log.Trace().Message("Запуск CallNumbersControlByMinuteCalls: " + DateTime.Now).Write();
            try
            {
                if (!CallNumbersControlByMinuteCallsProcessing) try
                    {
                        CallNumbersControlByMinuteCallsProcessing = true;

                        var callProjects = db.CallJobs
                            .Where(e => e.IsActive)
                            .Where(e => e.TimeStart <= DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second) && e.TimeStop > DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second))
                            .Select(e => e.CallProject)
                            .Where(e => e.CallBaseByMinuteCallsMonitoring)
                            .Distinct()
                            .ToList();

                        foreach (var callProject in callProjects)
                        {
                            NLog.Fluent.Log.Trace().Message("Запуск CallNumbersControlByMinuteCalls по проекту: " + callProject.Name + " " + DateTime.Now).Write();
                            CallBaseControlByMinuteCalls(callProject);
                        }
                    }
                    finally { CallNumbersControlByMinuteCallsProcessing = false; }
            }
            catch (Exception ex)
            {
                NLog.Fluent.Log.Error().Exception(ex).Write();
            }
        }

        public void CallBaseControlByMinuteCalls(CallProject callProject)
        {
            try
            {
                //Выбираем все активные базы по проекту без минутных разговоров
                var active_cbs = db.CallBases
                    .Where(e => e.CallProject_Id == callProject.Id)
                    .Where(e => e.IsActive)
                    .Where(e => e.TimeSwitchActive < DbFunctions.AddMinutes(DateTime.Now, -15))//DateTime.Now.AddMinutes(-15))
                    .Where(e => e.AmountMinuteCallsCurrentBase < 1)
                    .ToList();
                NLog.Fluent.Log.Trace().Message("Найдено баз для выключения: " + active_cbs.Count + " " + DateTime.Now).Write();
                //выключаем выбранные базы и проставляем время выключения
                foreach (var cb in active_cbs)
                {
                    NLog.Fluent.Log.Trace().Message("Выключаем базу: " + cb.Name + " " + DateTime.Now).Write();
                    cb.IsActive = false;
                    cb.OffTimeByMinuteCalls = DateTime.Now;
                    cb.TimeSwitchActive = null;
                    cb.OffByMinuteCallsCounter = cb.OffByMinuteCallsCounter == null ? 1 : cb.OffByMinuteCallsCounter + 1; 
                    db.SaveChanges();
                }

                //Выбираем все неактивные базы по проекту с минутными разговорами
                var unactiveBaseWithMinuteCalls = db.CallBases
                    .Where(e => e.CallProject_Id == callProject.Id)
                    .Where(e => e.IsActive == false)
                    .Where(e => e.OffTimeByMinuteCalls != null)
                    .Where(e => e.AmountMinuteCallsCurrentBase > 0)
                    .ToList();
                //включаем базы с минутными разговорами обратно
                foreach (var cb in unactiveBaseWithMinuteCalls)
                {
                    NLog.Fluent.Log.Trace().Message("Включаем базу с минутным разговором: " + cb.Name + " " + DateTime.Now).Write();
                    cb.IsActive = true;
                    cb.OffTimeByMinuteCalls = null;
                    cb.TimeSwitchActive = DateTime.Now;
                    db.SaveChanges();
                }

                //получаем кол-во линий по провайдерам
                var readyLines = db.Database.SqlQuery<ReadyByProvider>($@"
					select	cpr.Provider_Id, sum(isnull(csls.CallCount, 0) + isnull(csls.ReadyCount, 0)) Ready
					from	{db.DbName()}.dbo.CallProjectRoutes cpr
					join	{db.DbName()}.dbo.CallProjectRouteLines cprl
					on		cpr.Id = cprl.CallProjectRoute_Id left
					join	{db.DbName()}.dbo.CallServerLineStates csls
					on		cprl.Provider_Id = csls.Provider_Id 
					and		cpr.Prefix = csls.Prefix
					where	cpr.CallProject_Id = @CallProject_Id
					group	by cpr.Provider_Id
					",
                    new SqlParameter("@CallProject_Id", callProject.Id)).ToList();

                //получаем текущее кол-во номеров готовых для дозвона
                var readyNumbers = db.Database.SqlQuery<ReadyByProvider>($@"
					select	cbn.Provider_Id, count(0) Ready
					from	{db.DbName()}.dbo.CallBaseNumbers_{callProject.Id} cbn
					join	{db.DbName()}.dbo.CallBases cb
					on		cbn.CallBase_Id = cb.Id
					where	cb.IsActive = 1
					and		(cbn.State = 'N' or (cbn.State = 'D' and datediff(minute, isnull(cbn.LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= 30)
                            or (cbn.State = 'L' and datediff(minute, isnull(cbn.LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= 5)
                            or (cbn.State = 'T' and datediff(minute, isnull(cbn.LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= {callProject.RecallTime}))
					group	by cbn.Provider_Id
					");

                //по каждому провайдеру у которого есть линии
                foreach (var line in readyLines.Where(e => e.Ready != 0))
                {
                    //минимальное кол-во номеров
                    var least = line.Ready * callProject.CallBaseNumbersLevelLeast;
                    //требуемое кол-во номеров
                    var demand = line.Ready * callProject.CallBaseNumbersLevelDemand;
                    //граница выборки из архива
                    var range = callProject.CallBaseNumbersSampleRange;

                    if (range == 0) continue;

                    if (line.Provider_Id == null)
                    {

                    }
                    else
                    {
                        //текущее кол-во номеров по провайдеру
                        var numbers = readyNumbers.FirstOrDefault(e => e.Provider_Id == line.Provider_Id)?.Ready ?? 0;
                        //если текущее кол-во номеров меньше минимального
                        if (numbers < least)
                        {
                            //выбираем базы из выключенных по отсутствию минутных разговоров
                            var offCallBases = db.CallBases
                                .Where(e => e.CallProject_Id == callProject.Id)
                                .Where(e => e.IsActive == false)
                                .Where(e => e.Provider_Id == line.Provider_Id)
                                .Where(e => e.OffTimeByMinuteCalls != null 
                                && ((e.OffByMinuteCallsCounter >= 1 && e.OffByMinuteCallsCounter <= 2 && e.OffTimeByMinuteCalls < DbFunctions.AddHours(DateTime.Now, -6)) || 
                                (e.OffByMinuteCallsCounter >= 3 && e.OffByMinuteCallsCounter <= 4 && e.OffTimeByMinuteCalls < DbFunctions.AddHours(DateTime.Now, - 12)) ||
                                (e.OffByMinuteCallsCounter >= 5 && e.OffByMinuteCallsCounter <= 6 && e.OffTimeByMinuteCalls < DbFunctions.AddHours(DateTime.Now, - 24)) ||
                                (e.OffByMinuteCallsCounter > 6 && e.OffTimeByMinuteCalls < DbFunctions.AddHours(DateTime.Now, - 72))))
                                .ToList();

                            foreach(var cb in offCallBases)
                            {
                                if(numbers < demand)
                                {
                                    NLog.Fluent.Log.Trace().Message("Включаем базу: " + cb.Name + " " + DateTime.Now).Write();
                                    cb.IsActive = true;
                                    cb.OffTimeByMinuteCalls = null;
                                    cb.TimeSwitchActive = DateTime.Now;

                                    db.SaveChanges();

                                    numbers += (cb.Capacity - cb.Counter);
                                }
                               
                            }

                            //if(numbers < demand)
                            //{
                            //    RangeCopyBase(callProject.Id, line.Provider_Id.Value, demand - numbers, range, true, true, true, true, true, true, false, true, false, false, false, false, true, false, false, true);
                            //}
                        }
                            //RangeCopyBase(callProject.Id, line.Provider_Id.Value, demand - numbers, range, true, true, true, true, true, true, false, true, false, false, false, false, true, false, false, true);
                    }
                }
            }
            catch(Exception ex)
            {
                NLog.Fluent.Log.Error().Exception(ex).Write();
            }
        }

        #endregion

        #region .. CallNumbersControl ..

        public static bool CallNumbersControlProcessing { get; set; }

        public void CallNumbersControl()
        {
            try
            {
                if (!CallNumbersControlProcessing) try
                    {
                        CallNumbersControlProcessing = true;

                        var callProjects = db.CallJobs
                            .Where(e => e.IsActive)
                            .Where(e => e.TimeStart <= DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second) && e.TimeStop > DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second))
                            .Select(e => e.CallProject)
                            .Distinct()
                            .ToList();

                        foreach (var callProject in callProjects)
                        {
                            switch (callProject.ControlType)
                            {
                                case CallProjectControlType.NumberLevel:
                                    CallNumbersControlByNumberLevel(callProject);
                                    break;
                                case CallProjectControlType.BaseQuality:
                                    CallNumbersControlByBaseQuality(callProject);
                                    break;
                            }
                        }
                    }
                    finally { CallNumbersControlProcessing = false; }
            }
            catch (Exception ex) 
            {
                NLog.Fluent.Log.Error().Exception(ex).Write();
            }
        }

        class ReadyByProvider { public long? Provider_Id { get; set; } public int Ready { get; set; } }

        public void CallNumbersControlByBaseQuality(CallProject callProject)
        {
            try
            {
                // 1. оценка/отключение активных баз
                var active_cbs = db.CallBases
                    .Where(e => e.CallProject_Id == callProject.Id)
                    .Where(e => e.IsActive)
                    .Where(e => e.QualityCounter >= callProject.BaseQualityCounter)
                    .ToList();

                foreach(var cb in active_cbs)
                {
                    // общий процент качественных
                    var total = db.Database.SqlQuery<int>($@"
                        select	sum(cou)
                        from	(
		                        select	count(0) cou
		                        from	SelenaCRM.dbo.CallBaseNumbers_{callProject.Id}
		                        where	CallBase_Id = @CallBase_Id
		                        and		State = 'C'
		                        and		Length > 60
		                        union	all
		                        select	count(0) cou
		                        from	SelenaCRM.dbo.CallBaseNumbers_arch_{callProject.Id}
		                        where	CallBase_Id = @CallBase_Id
		                        and		Length > 60
		                        ) x
                        ",
                        new SqlParameter("@CallBase_Id", cb.Id)).First();

                    // качественная - пропускаем
                    if (1.0 * total / cb.Capacity >= callProject.BaseQualitySuccessPercent) continue;

                    // наличие качественных по счетчику деактивации
                    var count = db.Database.SqlQuery<int>($@"
                        select	count(0)
                        from	(
		                        select	top({cb.QualityCounter}) Length
		                        from	{db.DbName()}.dbo.CallBaseNumbers_{callProject.Id}
		                        where	CallBase_Id = @CallBase_Id
		                        and		State = 'C'
		                        order	by LastAttemptDate desc
		                        ) x
                        where	Length > 60
                        ",
                        new SqlParameter("@CallBase_Id", cb.Id)).First();

                    // нет качественных - отключаем
                    if (count == 0)
                    {
                        cb.IsActive = false;
                        cb.QualityOffTime = DateTime.Now.TimeOfDay;
                        db.SaveChanges();
                    }

                    // достигнут лимит по проценту обработки - в архив
                    if (1.0 * cb.Counter / cb.Capacity >= callProject.BaseQualityLeastPercent)
                    {
                        db.Database.ExecuteSqlCommand($@"
							begin transaction
							begin try

							insert  {db.DbName()}.dbo.CallBaseNumbers_arch_{callProject.Id} (AttemptsQty, CallBase_Id, Confirmed, LastAttemptDate, Length, Operator_Id, Phone, Result, Descr, Provider_Id)
							select  AttemptsQty, CallBase_Id, Confirmed, LastAttemptDate, Length, Operator_Id, Phone, Result, Descr, Provider_Id
							from    {db.DbName()}.dbo.CallBaseNumbers_{callProject.Id}
							where   CallBase_Id = @CallBase_Id

							delete  {db.DbName()}.dbo.CallBaseNumbers_{callProject.Id}
							where   CallBase_Id = @CallBase_Id

							update	{db.DbName()}.dbo.CallBases
							set		Counter = Capacity,
									IsActive = 0,
									SwitchTime = null
							where   Id = @CallBase_Id

							end try
							begin catch
								declare @ErrMsg nvarchar(2048)
								select @ErrMsg = ERROR_MESSAGE()
								if @@TRANCOUNT > 0 rollback transaction
									RAISERROR(@ErrMsg, 15, 1)
							end catch
							if @@TRANCOUNT > 0 commit transaction
							", new SqlParameter("@CallBase_Id", cb.Id));
                    }
                }

                // 2. список направлений, для которых нет активных баз
                var provider_ids = db.CallBases
                    .Where(e => e.CallProject_Id == callProject.Id)
                    .Where(e => e.Provider_Id.HasValue).Select(e => e.Provider_Id).Distinct()
                    .Except(db.CallBases
                        .Where(e => e.IsActive)
                        .Select(e => e.Provider_Id).Distinct())
                    .ToList();

                // 3. активация
                foreach (var provider_id in provider_ids)
                {
                    // 3.1. активация отключенных контролем качества
                    var timeNow = DateTime.Now.TimeOfDay;
                    var incative_cb = db.CallBases
                        .Where(e => e.CallProject_Id == callProject.Id)
                        .Where(e => !e.IsActive)
                        .Where(e => (timeNow < callProject.BaseQualityShiftSeparator && e.QualityOffTime >= callProject.BaseQualityShiftSeparator) || (timeNow >= callProject.BaseQualityShiftSeparator && e.QualityOffTime < callProject.BaseQualityShiftSeparator))
                        .OrderBy(e => new Guid())
                        .FirstOrDefault();

                    if (incative_cb != null)
                    {
                        incative_cb.IsActive = true;
                        incative_cb.QualityCounter = 0;
                        incative_cb.QualityOffTime = null;
                        db.SaveChanges();
                    }
                    else
                    {
                        // 3.2. активация архивных
                    }

                }
            }
            catch (Exception ex)
            {
                NLog.Fluent.Log.Error().Message($"{callProject.Name}: failed").Exception(ex).Write();
            }
        }

        public void CallNumbersControlByNumberLevel(CallProject callProject)
        {
            try
            {
                // автоотключение по назначениям
                db.Database.ExecuteSqlCommand($@"
					update {db.DbName()}.dbo.CallBases
					set		IsActive = 0,
							SwitchTime = getDate()
					where	Id in 
							(
								select	cb.Id
								from	{db.DbName()}.dbo.CallBases cb
								join	{db.DbName()}.dbo.CallProjects cp
								on		cb.CallProject_Id = cp.Id
								where	cp.Id = @CallProject_Id
								and     cb.IsActive = 1
								and		dateDiff(minute, cb.LastAssignDate, getDate()) > cp.CbSwitchOff_MaxAssignTime
								and		dateDiff(minute, cp.TimeStart, cast(cb.LastAssignDate as time)) > cp.CbSwitchOff_MaxAssignTime
								and		cp.CbSwitchOff_MaxAssignTime > 0
							)
					",
                    new SqlParameter("@CallProject_Id", callProject.Id));

                // автоотключение по коду "55"
                db.Database.ExecuteSqlCommand($@"
					update {db.DbName()}.dbo.CallBases
					set		IsActive = 0,
							SwitchTime = getDate()
					where	Id in 
							(
								select	Id
								from	(
											select	cb.Id, count(0) Counter
											from	{db.DbName()}.dbo.CallBases cb
											join	(select CallBase_Id from {db.DbName()}.dbo.CallBaseNumbers_{callProject.Id} where Descr = '55' union all select CallBase_Id from {db.DbName()}.dbo.CallBaseNumbers_arch_{callProject.Id} where Descr = '55') cbn
											on		cb.Id = cbn.CallBase_Id
											where	cb.IsActive = 1
											and		cb.CallProject_Id = @CallProject_Id
											group	by cb.Id
										) x
								where	Counter >= @Limit
								and		@Limit > 0
							)
					", 
                    new SqlParameter("@CallProject_Id", callProject.Id),
                    new SqlParameter("@Limit", callProject.CbSwitchOff_Dtmf55Limit));

                // автоотключение по коду "44"
                db.Database.ExecuteSqlCommand($@"
					update {db.DbName()}.dbo.CallBases
					set		IsActive = 0,
							SwitchTime = getDate()
					where	Id in 
							(
								select	Id
								from	(
											select	cb.Id, count(0) Counter
											from	{db.DbName()}.dbo.CallBases cb
											join	(select CallBase_Id from {db.DbName()}.dbo.CallBaseNumbers_{callProject.Id} where Descr = '44' union all select CallBase_Id from {db.DbName()}.dbo.CallBaseNumbers_arch_{callProject.Id} where Descr = '44') cbn
											on		cb.Id = cbn.CallBase_Id
											where	cb.IsActive = 1
											and		cb.CallProject_Id = @CallProject_Id
											group	by cb.Id
										) x
								where	Counter >= @Limit
								and		@Limit > 0
							)
					",
                    new SqlParameter("@CallProject_Id", callProject.Id),
                    new SqlParameter("@Limit", callProject.CbSwitchOff_Dtmf44Limit));

                // удаление баз с емкостью 0 через 30 мин
                db.Database.ExecuteSqlCommand($@"
                    delete
                    from    {db.DbName()}.dbo.CallBases
                    where   CallProject_Id = @CallProject_Id
                            and Capacity = 0
                            and LoadDate < Dateadd(minute, -30, GetDate())",
                    new SqlParameter("@CallProject_Id", callProject.Id));

                // автоотключение по проценту обработки > 93%
                db.Database.ExecuteSqlCommand($@"
					update {db.DbName()}.dbo.CallBases
					set		IsActive = 0,
							SwitchTime = getDate()
					where	Id in 
							(
								select	Id
								from	{db.DbName()}.dbo.CallBases
								where	CallProject_Id = @CallProject_Id
								and		IsActive = 1
								and		Counter * 1.0 / Capacity > 0.93
							)
                    and     @CbSwitchOff_CounterPercentLimit = 1
					",
                    new SqlParameter("@CallProject_Id", callProject.Id),
                    new SqlParameter("@CbSwitchOff_CounterPercentLimit", callProject.CbSwitchOff_CounterPercentLimit));

                // автоотключение по остатку (обработанные)
                db.Database.ExecuteSqlCommand($@"
					update {db.DbName()}.dbo.CallBases
					set		IsActive = 0,
							SwitchTime = getDate()
					where	Id in 
							(
								select	Id
								from	{db.DbName()}.dbo.CallBases
								where	CallProject_Id = @CallProject_Id
								and		IsActive = 1
								and		Id not in (select distinct CallBase_Id from {db.DbName()}.dbo.CallBaseNumbers_{callProject.Id} where State = 'N' or State = 'L' or State = 'T' or State = 'D' or State = 'P')
							)
					",
                    new SqlParameter("@CallProject_Id", callProject.Id));

                //and		Id not in (select distinct CallBase_Id from {db.DbName()}.dbo.CallBaseNumbers_{callProject.Id} where State = 'N' or State = 'L' or (State = 'T' and datediff(minute, LastAttemptDate, getdate()) >= {callProject.RecallTime}) or (State = 'D' and datediff(minute, LastAttemptDate, getdate()) >= 30))

                // автоархивация деактивированных баз то таймауту
                var processedBases = db.Database.SqlQuery<long>($@"
					select	Id
					from	{db.DbName()}.dbo.CallBases
					where	CallProject_Id = @CallProject_Id
					and		IsActive = 0
					and		dateDiff(minute, SwitchTime, getDate()) >= 30
					",
                    new SqlParameter("@CallProject_Id", callProject.Id))
                    .ToList();

                foreach(var id in processedBases)
                {
                    db.Database.ExecuteSqlCommand($@"
						begin transaction
						begin try

						insert  {db.DbName()}.dbo.CallBaseNumbers_arch_{callProject.Id} (AttemptsQty, CallBase_Id, Confirmed, LastAttemptDate, Length, Operator_Id, Phone, Result, Descr, Provider_Id)
						select  AttemptsQty, CallBase_Id, Confirmed, LastAttemptDate, Length, Operator_Id, Phone, Result, Descr, Provider_Id
						from    {db.DbName()}.dbo.CallBaseNumbers_{callProject.Id}
						where   CallBase_Id = @CallBase_Id

						delete  {db.DbName()}.dbo.CallBaseNumbers_{callProject.Id}
						where   CallBase_Id = @CallBase_Id

						update	{db.DbName()}.dbo.CallBases
						set		Counter = Capacity,
								IsActive = 0,
                                SwitchTime = null
						where   Id = @CallBase_Id

						end try
						begin catch
							declare @ErrMsg nvarchar(2048)
							select @ErrMsg = ERROR_MESSAGE()
							if @@TRANCOUNT > 0 rollback transaction
								RAISERROR(@ErrMsg, 15, 1)
						end catch
						if @@TRANCOUNT > 0 commit transaction
						", new SqlParameter("@CallBase_Id", id));
                }

                // автоактивация
                if (!callProject.CallBasesAutoActivation) return;

                var readyLines = db.Database.SqlQuery<ReadyByProvider>($@"
					select	cpr.Provider_Id, sum(isnull(csls.CallCount, 0) + isnull(csls.ReadyCount, 0)) Ready
					from	{db.DbName()}.dbo.CallProjectRoutes cpr
					join	{db.DbName()}.dbo.CallProjectRouteLines cprl
					on		cpr.Id = cprl.CallProjectRoute_Id left
					join	{db.DbName()}.dbo.CallServerLineStates csls
					on		cprl.Provider_Id = csls.Provider_Id
					and		cpr.Prefix = csls.Prefix
					where	cpr.CallProject_Id = @CallProject_Id
					group	by cpr.Provider_Id
					",
                    new SqlParameter("@CallProject_Id", callProject.Id)).ToList();

                var readyNumbers = db.Database.SqlQuery<ReadyByProvider>($@"
					select	cbn.Provider_Id, count(0) Ready
					from	{db.DbName()}.dbo.CallBaseNumbers_{callProject.Id} cbn
					join	{db.DbName()}.dbo.CallBases cb
					on		cbn.CallBase_Id = cb.Id
					where	cb.IsActive = 1
					and		(cbn.State = 'N' or (cbn.State = 'D' and datediff(minute, isnull(cbn.LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= 30)
                            or (cbn.State = 'L' and datediff(minute, isnull(cbn.LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= 5)
                            or (cbn.State = 'T' and datediff(minute, isnull(cbn.LastAttemptDate, dateAdd(day, -1, getDate())), getdate()) >= {callProject.RecallTime}))
					group	by cbn.Provider_Id
					");

                foreach (var line in readyLines.Where(e => e.Ready != 0))
                {
                    var least = line.Ready * callProject.CallBaseNumbersLevelLeast;
                    var demand = line.Ready * callProject.CallBaseNumbersLevelDemand;
                    var range = callProject.CallBaseNumbersSampleRange;

                    if (range == 0) continue;

                    if (line.Provider_Id == null)
                    {
                        
                    }
                    else
                    {
                        var numbers = readyNumbers.FirstOrDefault(e => e.Provider_Id == line.Provider_Id)?.Ready ?? 0;
                        if (numbers < least) RangeCopyBase(callProject.Id, line.Provider_Id.Value, demand - numbers, range, true, true, true, true, true, true, false, true, false, false, false, false, true, false, false, true);
                    }
                }

                /*if (callProject.CallBasesAutoActivation) db.Database.ExecuteSqlCommand($@"
					declare @ReadyNumbers int, @CBID bigint

					select	@ReadyNumbers = count(0)
					from	{db.DbName()}.dbo.CallBaseNumbers_{callProject.Id} cbn,
							{db.DbName()}.dbo.CallBases cb
					where	not exists (select * from {db.DbName()}.dbo.CallBaseNumbers_process_{callProject.Id} p where p.Id = cbn.Id)
					and		(cbn.State = 'N' or (cbn.State = 'T' and datediff(hour, cbn.LastAttemptDate, getdate()) >= 6) or (cbn.State = 'L' and datediff(minute, cbn.LastAttemptDate, getdate()) >= 5) or (cbn.State = 'D' and datediff(minute, cbn.LastAttemptDate, getdate()) >= 30))
					and		cbn.CallBase_Id = cb.Id
					and		cb.IsActive = 1
					and		cb.CallProject_Id = @CallProject_Id

					while (@CallBasesAutoActivationLeast > @ReadyNumbers)
					begin
						select @CBID = null

						select top(1) @CBID = cb.Id, @ReadyNumbers = @ReadyNumbers + Available
						from	(
									select	Id,
											Confirmed,
											Counter Completed,
											Capacity - Counter Available,
											Capacity Total,
											dateadd(hour, @ShortCallsAutoDeActivationMinDuration, isnull(ShortCallsAutoDeActivationTime, '1900-01-01')) AllowedActivationTime
									from	{db.DbName()}.dbo.CallBases
									where	IsActive = 0
									and		Capacity > Counter
								) cb
						where	cb.AllowedActivationTime < getdate()
						and		case when cb.Total = 0 then 0 else 1.0 * cb.Completed / cb.Total end > @CallBasesAutoActivationPercent
						and		cb.Available > 0
						order	by case when cb.Completed = 0 then 1 else 1.0 * cb.Confirmed / cb.Completed end desc

						if (isnull(@CBID, 0) = 0)
						select	@ReadyNumbers = @CallBasesAutoActivationLeast else
						update	{db.DbName()}.dbo.CallBases
						set		IsActive = 1,
								LastAssignDate = getdate()
						where	Id = @CBID
					end
					",*/
                    //new SqlParameter("@CallProject_Id", callProject.Id),
                    //new SqlParameter("@CallBasesAutoActivationPercent", callProject.CallBasesAutoActivationPercent),
                    //new SqlParameter("@ShortCallsAutoDeActivationMinDuration", /*callProject.ShortCallsAutoDeActivationMinDuration*/0),
                    //new SqlParameter("@CallBasesAutoActivationLeast", callProject.CallBasesAutoActivationLeast));
            }
            catch (Exception ex) 
            {
                NLog.Fluent.Log.Error().Message($"{callProject.Name}: failed").Exception(ex).Write();
            }
        }

        void RangeCopyBase(long project_id, long provider_id, int numbersCount, int sampleRange, bool copy_N, bool copy_TP, bool copy_DL, bool copy_O, bool copy_C, bool copy_null, bool copy_00, bool copy_11, bool copy_33, bool copy_44, bool copy_55, bool copy_66, bool copy_77, bool copy_88, bool copy_99, bool clientsExclude)
        {
            var franchisee_id = db.CallProjects.AsNoTracking().First(e => e.Id == project_id).Franchisee_Id;
            // максимальное кол-во оперативных номеров
            var callProjectMaxNumbersCount = int.Parse(db.Options.First(x => x.Name == Option.CallProjectMaxNumbersCount).Value);
            // максимальное кол-во оперативных номеров - превышение
            if (db.Database.SqlQuery<int>($"select count(0) from {db.DbName()}.dbo.CallBaseNumbers_{project_id}").First() > callProjectMaxNumbersCount) throw new Exception(string.Format(Strings.CallBase_Error_CallProjectMaxNumbersCount, db.CallProjects.Find(project_id).Name, callProjectMaxNumbersCount));

            var states = string.Join(",", new[] { copy_N ? "'N'" : null, copy_TP ? "'T','P'" : null, copy_DL ? "'D','L'" : null, copy_O ? "'O'" : null, copy_C ? "'C'" : null }.Where(e => e != null));
            var codes = string.Join(",", new[] { copy_11 ? "'11'" : null, copy_33 ? "'33'" : null, copy_44 ? "'44'" : null, copy_55 ? "'55'" : null, copy_66 ? "'66'" : null, copy_77 ? "'77'" : null, copy_88 ? "'88'" : null, copy_99 ? "'99'" : null }.Where(e => e != null));
            var dateMax = db.CallBases.Where(e => e.CallProject_Id == project_id).Where(e => e.Capacity == e.Counter).Where(e => e.Provider_Id == provider_id).Min(e => e.LoadDate).AddDays(sampleRange);

            var where = string.Join(" or ", new[] { string.IsNullOrEmpty(states) ? null : $"State in ({states})", string.IsNullOrEmpty(codes) ? null : $"isnull(Descr, '') in ({codes})", copy_00 ? "Confirmed = 1" : null }.Where(e => e != null));
            var where_arch = string.Join(" or ", new[] { string.IsNullOrEmpty(codes) ? null : $"isnull(Descr, '') in ({codes})", copy_00 ? "Confirmed = 1" : null, copy_null ? "(Descr is null and isnull(Confirmed, 0) = 0)" : null }.Where(e => e != null));

            var old_CallBases = db.CallBases
                .Where(e => e.CallProject_Id == project_id)
                .Where(e => e.Provider_Id == provider_id)
                .Where(e => e.LoadDate < dateMax)
                .Where(e => !e.Name.StartsWith("#"))
                .OrderBy(e => Guid.NewGuid())
                .Select(entity => new
                {
                    entity.Id,
                    entity.CallProject_Id,
                    entity.Provider_Id,
                    entity.Name,
                    entity.Description,
                    entity.NumbersFileName,
                    entity.Marker,
                    entity.AmountMinuteCallsCurrentBase,
                    entity.AmountMinuteCallsBeforeBase,
                    entity.AmountMinuteCallsHistory,
                    entity.LoadDate
                })
                .ToList();

            foreach (var old_CallBase in old_CallBases)
            {
                var numbers = db.Database.SqlQuery<string>($@"
					select Phone from {db.DbName()}.dbo.CallBaseNumbers_{project_id} where CallBase_Id = @CallBase_Id and ({(string.IsNullOrEmpty(where) ? "1 = 0" : where)})
					union all
					select Phone from {db.DbName()}.dbo.CallBaseNumbers_arch_{project_id} where CallBase_Id = @CallBase_Id and ({(string.IsNullOrEmpty(where_arch) ? "1 = 0" : where_arch)})
					", new SqlParameter("@CallBase_Id", old_CallBase.Id)).ToList();

                if (clientsExclude) numbers = numbers.Except(db.Clients.Where(e => e.Status == ClientStatus.Appeared || e.Status == ClientStatus.Canceled || e.Status == ClientStatus.Completed /*|| e.Status == ClientStatus.ValidBlackList*/ || e.Status == ClientStatus.Started || e.Status == ClientStatus.Selling /*|| e.Status == ClientStatus.VipFreeScreening*/ || e.Status == ClientStatus.Applying /*|| e.Status == ClientStatus.BankRefusing || e.Status == ClientStatus.Returned*/).Select(e => e.Phone).Distinct().ToList()).ToList();

                var new_CallBase = new CallBase
                {
                    CallProject_Id = old_CallBase.CallProject_Id,
                    Provider_Id = old_CallBase.Provider_Id,
                    Name = old_CallBase.Name,
                    Description = old_CallBase.Description,
                    IsActive = false,
                    NumbersFileName = old_CallBase.NumbersFileName,
                    NumbersFileData = NumbersFile.ContentToData(string.Join("\r\n", numbers), true),
                    LoadDate = DateTime.Now,
                    Capacity = -1,
                    Counter = 0,
                    Confirmed = 0,
                    CheckDoubles = false,
                    LastAssignDate = DateTime.Now,
                    ShortCallsAutoDeActivationTime = null,
                    Marker = old_CallBase.Marker,
                    AmountMinuteCallsCurrentBase = null,
                    AmountMinuteCallsBeforeBase = old_CallBase.AmountMinuteCallsCurrentBase,
                    AmountMinuteCallsHistory = (string.IsNullOrWhiteSpace(old_CallBase.AmountMinuteCallsHistory) ? string.Empty : old_CallBase.AmountMinuteCallsHistory) + old_CallBase.LoadDate.ToString("yyyy.MM.dd hh:mm:ss") + " " + old_CallBase.AmountMinuteCallsCurrentBase + "\t"
                };

                var remove_CallBase = new CallBase { Id = old_CallBase.Id };

                // insert в CallBases
                db.CallBases.Add(new_CallBase);
                // remove из CallBases
                db.CallBases.Attach(remove_CallBase);
                db.CallBases.Remove(remove_CallBase);
                db.SaveChanges();

                var new_logCopyBase = new LogCopyBase
                {
                    Name = new_CallBase.Name,
                    Date = new_CallBase.LoadDate,
                    Options = $"{Strings.CallBase_Tools_CopyBase_NumbersCount}: {numbersCount}, {Strings.CallBase_Tools_CopyBase_SampleRange}: {sampleRange}" + "\nСтатусы: " + (copy_N ? " Новый," : "") + (copy_TP ? " Недозвон," : "") + (copy_DL ? " Нет линий," : "") + (copy_O ? " Пропущен," : "") + (copy_C ? " Отработан," : "") + "\nКоды операторов: " + (copy_null ? " Пусто," : "") + (copy_00 ? " 00," : "") + (copy_33 ? " 33," : "") + (copy_44 ? " 44," : "") + (copy_55 ? " 55," : "") + (copy_66 ? " 66," : "") + (copy_77 ? " 77," : "") + (copy_88 ? " 88," : "") + (copy_99 ? " 99," : "") + "\n" + (clientsExclude ? Strings.CallBase_CopyBase_Message_ExcludePhoneClient : ""),
                    Description = "Проект дозвона: " + new_CallBase.CallProject_Id + " \nЕмкость: " + numbers.Count.ToString() + " \nПровайдер: " + db.Providers.Where(e => e.Id == new_CallBase.Provider_Id).FirstOrDefault().Name + " \nСкопированно с базы: " + old_CallBase.Id + " " + old_CallBase.Name + " " + old_CallBase.Description + " \nКопию сделал: " + Strings.CallProject_DataGroup_CallBasesAutoActivation,
                    Result = true
                };

                // insert в LogCopyBase
                db.LogCopyBases.Add(new_logCopyBase);
                db.SaveChanges();

                CallBaseController.SaveNumbers(db, new_CallBase.CallProject_Id, new_CallBase.Id, NumbersFile.ZipToContent(new_CallBase.NumbersFileData), new_CallBase.CheckDoubles, true);

                if ((numbersCount -= numbers.Count) < 0) break;
            }
        }

        #endregion

        #region .. CallBaseChecker ..

        public static bool CallBaseCheckerProcessing { get; set; }

        public void CallBaseChecker()
        {
            try
            {
                if (!CallBaseCheckerProcessing) try
                    {
                        CallBaseCheckerProcessing = true;

                        var callProjects = db.CallJobs
                            .Where(e => e.IsActive)
                            .Where(e => e.TimeStart <= DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second) && e.TimeStop > DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second))
                            .Select(e => e.CallProject)
                            .Distinct()
                            .ToList();

                        foreach (var callProject in callProjects)
                        {
                            CallBaseChecker(callProject);
                        }
                    }
                    finally { CallBaseCheckerProcessing = false; }
            }
            catch (Exception ex) { }
        }

        public void CallBaseChecker(CallProject callProject)
        {
            try
            {
                db.Database.ExecuteSqlCommand($@"
					declare @tab table(CallBase_Id bigint, Provider_Id bigint, cou int)
					declare @new table(CallBase_Id bigint, Provider_Id bigint)

					insert	@tab
					select	x.CallBase_Id, x.Provider_Id, count(0) cou
					from	(
							select	CallBase_Id, Provider_Id
							from	{db.DbName()}.dbo.CallBaseNumbers_{callProject.Id}
							union	all
							select	CallBase_Id, Provider_Id
							from	{db.DbName()}.dbo.CallBaseNumbers_arch_{callProject.Id}
							) x
					group	by x.CallBase_Id, x.Provider_Id

					insert @new
					select	t.CallBase_Id, min(t.Provider_Id) Provider_Id
					from	(select CallBase_Id, max(cou) cou from @tab group by CallBase_Id) cb
					join	@tab t
					on		t.CallBase_Id = cb.CallBase_Id
					and		t.cou = cb.cou
					group	by t.CallBase_Id

					update	{db.DbName()}.dbo.CallBases
					set		Provider_Id = x.Provider_Id
					from	@new x
					where	x.CallBase_Id = Id
					");

            }
            catch (Exception ex)
            {
                NLog.Fluent.Log.Error().Message($"{callProject.Name}: failed").Exception(ex).Write();
            }
        }

        #endregion

        #region .. common ..

        class CBNumber
        {
            public long Id { get; set; }
            public string Phone { get; set; }
            public long? Provider_Id { get; set; }
        }

        class CBPacketNumber
        {
            public long Id { get; set; }
            public string CallerID { get; set; }
            public string Phone { get; set; }
        }

        class CBPacketLine
        {
            public string Line { get; set; }
        }

        class CallBasePacket
        {
            public string Sender { get; set; }
            public List<CBPacketLine> Lines { get; set; }
            public List<CBPacketNumber> Numbers { get; set; }
            public long CallProject_Id { get; set; }
            public long CallJob_Id { get; set; }
            public string ScriptName { get; set; }
            public int DialTime { get; set; }
            public int Interval { get; set; }
        }

        class CheckBasePacket
        {
            public string Sender { get; set; }
            public List<CBPacketLine> Lines { get; set; }
            public List<CBPacketNumber> Numbers { get; set; }
            public long CheckProject_Id { get; set; }
            public int Interval { get; set; }
            public int WithIVR { get; set; }
        }

        class CSRate
        {
            public CallServer CallServer { get; set; }
            public string Line { get; set; }
            public double Rate { get; set; }
        }

        #endregion

        // tmp !!! только для oktell
        #region .. CallConnectionFiller ..

        public static bool CallConnectionFillerProcessing { get; set; }

        public void CallConnectionFiller()
        {
            try
            {
                if (!CallConnectionFillerProcessing) try
                    {
                        CallConnectionFillerProcessing = true;

                        var callServers = db.CallServers.ToList();

                        foreach (var callServer in callServers) if (callServer.Code.ToLower().Contains("oktell") && callServer.IsActive) CallConnectionFiller(callServer);
                    }
                    finally { CallConnectionFillerProcessing = false; }
            }
            catch (Exception ex) { }
        }

        [System.ComponentModel.DataAnnotations.Schema.NotMapped]
        class CallServerDb : DbContext { public CallServerDb(string connectionString) : base(connectionString) { } }

        void CallConnectionFill(CallServerDb cs_db, long callServer_Id, DateTime timeStart, DateTime timeStop)
        {
            var connections = cs_db.Database.SqlQuery<stor.CallConnection>($@"
				select	sc.IdChain Uuid,
						@callServer_Id CallServer_Id,
						sc.TimeStart,
						sc.TimeAnswer,
						sc.TimeTransfer,
						sc.TimeStop,
						sc.CalledId,
						sc.CallerId,
						sg.Name Gate
				from	(
						select	IdChain,
								max(case when ConnectionType = 7 then BOutNumber end) CallerId,
								max(case when ConnectionType = 5 then BOutNumber end) CalledId,
								max(case when ConnectionType = 7 then TimeStart end) TimeStart,
								max(case when ConnectionType = 7 then TimeAnswer end) TimeAnswer,
								max(case when ConnectionType = 5 then TimeAnswer end) TimeTransfer,
								isnull(max(TimeStop), max(case when ConnectionType = 7 then TimeAnswer end)) TimeStop,
								max(case when ConnectionType = 7 then BLineId end) LineId
						from	oktell.dbo.A_Stat_Connections_1x1
						where	TimeStart between @timeStart and @timeStop
								and ConnectionType in (7, 5)
						group	by IdChain
						) sc
				join	oktell_settings.dbo.A_ServerExtLines sel
				on		sc.LineId = sel.ID
				join	oktell_settings.dbo.A_ServerStreams ss
				on		sel.StreamId = ss.Id
				join	oktell_settings.dbo.A_ServerGates sg
				on		ss.GateId = sg.Id
				order	by TimeStart
				",
                new SqlParameter("@callServer_Id", callServer_Id),
                new SqlParameter("@timeStart", timeStart),
                new SqlParameter("@timeStop", timeStop))
                .ToList();

            if (connections.Count != 0)
            {
                var sqlDate = new Func<DateTime?, string>((value) => value.HasValue ? $"datetimeFromParts({value.Value.Year}, {value.Value.Month}, {value.Value.Day}, {value.Value.Hour}, {value.Value.Minute}, {value.Value.Second}, {value.Value.Millisecond})" : "null");
                var sqlString = new Func<string, string>((value) => string.IsNullOrEmpty(value) ? "null" : $"'{value}'");

                var script = new List<string>();
                connections.ForEach(c => script.Add($"'{c.Uuid}', {c.CallServer_Id}, {sqlDate(c.TimeStart)}, {sqlDate(c.TimeAnswer)}, {sqlDate(c.TimeTransfer)}, {sqlDate(c.TimeStop)}, {sqlString(c.CalledId)}, {sqlString(c.CallerId)}, {(int)stor.CallConnectionType.Outgoing}, {sqlString(c.Gate)}"));
                var sql = string.Join("\r\n", script.Select(s => $"insert CallConnections (Uuid, CallServer_Id, TimeStart, TimeAnswer, TimeTransfer, TimeStop, CalledId, CallerId, ConnectionType, Gate) values ({s})"));
                storage.Database.ExecuteSqlCommand(sql);
            }
        }

        public void CallConnectionFiller(CallServer callServer)
        {
            try
            {
                var cs = new SqlConnectionStringBuilder(db_ok_set.Database.Connection.ConnectionString);
                cs.DataSource = $"{new Uri(callServer.URL).Host},{cs.DataSource.Split(',')[1]}";

                using (var cs_db = new CallServerDb(cs.ToString()))
                {
                    var timeStop = cs_db.Database.SqlQuery<DateTime>("select datetimeFromParts(datePart(year, getDate()), datePart(month, getDate()), datePart(day, getDate()), datePart(hour, getDate()), 0, 0, 0)").First();
                    var timeStart = timeStop.AddHours(-1);
                    if (storage.CallConnections.Where(e => e.CallServer_Id == callServer.Id).Count() == 0 || storage.CallConnections.Where(e => e.CallServer_Id == callServer.Id).Max(e => e.TimeStart) < timeStart) CallConnectionFill(cs_db, callServer.Id, timeStart, timeStop);

                    //storage.Database.ExecuteSqlCommand("delete SelenaSTOR.dbo.CallConnections where ...");
                    //for (int day = 21; day <= 21; day++) for (int hour = 0; hour <= 23; hour++)
                    //    {
                    //        if (day == 21 && hour >= 13) break;
                    //        var timeStart = new DateTime(2018, 12, day, hour, 0, 0);
                    //        CallConnectionFill(cs_db, callServer.Id, timeStart.AddMilliseconds(100), timeStart.AddHours(1));
                    //    }

                    //storage.Database.ExecuteSqlCommand("delete SelenaSTOR.dbo.CallConnections where TimeStart > '2019-03-01'");
                    //for (int day = 1; day <= 6; day++) for (int hour = 0; hour <= 23; hour++)
                    //    {
                    //        if (day == 6 && hour >= 8) break;
                    //        var timeStart = new DateTime(2019, 3, day, hour, 0, 0);
                    //        CallConnectionFill(cs_db, callServer.Id, timeStart.AddMilliseconds(100), timeStart.AddHours(1));
                    //    }
                }
            }
            catch (Exception ex) { }
        }

        #endregion

        #region .. CPTest ..

        class CPTestGetModel
        {
            /// <summary>
            /// Проект дозвона
            /// </summary>
            public long CallProject_Id { get; set; }

            /// <summary>
            /// Название
            /// </summary>
            public string Name { get; set; }

            /// <summary>
            /// Шаги
            /// </summary>
            public List<CPTestStepModel> Steps { get; set; }
        }

        class CPTestStepModel
        {
            /// <summary>
            /// Порядковый номер
            /// </summary>
            public int Index { get; set; }

            /// <summary>
            /// Название
            /// </summary>
            public string Name { get; set; }

            /// <summary>
            /// Шаблон вопроса
            /// </summary>
            public string QueryPattern { get; set; }

            /// <summary>
            /// Файл ответа
            /// </summary>
            public Guid? AnswerDocument_Id { get; set; }
        }

        /// <summary>
        /// Возвращает свойства теста и список вопросов
        /// </summary>
        /// <param name="phone">Номер телефона</param>
        [HttpGet]
        [AllowAnonymous]
        public ActionResult CPTestGet(string phone)
        {
            try
            {
                var test = db.CallProjectTestNumbers.FirstOrDefault(e => e.Phone == phone)?.CallProjectTest;

                if (test == null) return new HttpStatusCodeResult(HttpStatusCode.NotFound);

                var data = new CPTestGetModel
                {
                    CallProject_Id = test.CallProject_Id,
                    Name = test.Name,
                    Steps = test.CallProjectTestSteps.Select(step => new CPTestStepModel
                    {
                        Index = step.Index,
                        Name = step.Name,
                        QueryPattern = step.QueryPattern,
                        AnswerDocument_Id = step.AnswerDocument_Id
                    }).ToList()
                };

                return Json(data);
            }
            catch (Exception ex)
            {
                return new HttpStatusCodeResult(HttpStatusCode.InternalServerError, ex.Message);
            }
        }

        /// <summary>
        /// Возвращает файл ответа на вопрос теста
        /// </summary>
        /// <param name="id">AnswerDocument_Id</param>
        [HttpGet]
        [AllowAnonymous]
        public ActionResult CPTestAnswer(Guid id)
        {
            try
            {
                var content = documents.DocumentContents.Where(e => e.DocumentHeader_Id == id && e.Data != null).OrderByDescending(e => e.Id).FirstOrDefault();

                if (content == null) return new HttpStatusCodeResult(HttpStatusCode.NotFound);

                var data = content.Attributes == DocumentContentAttributes.Compressed ? Zip.Decompress(content.Data) : content.Data;

                return File(data, System.Net.Mime.MediaTypeNames.Application.Octet, content.DocumentHeader.Name);
            }
            catch (Exception ex)
            {
                return new HttpStatusCodeResult(HttpStatusCode.InternalServerError, ex.Message);
            }
        }

        /// <summary>
        /// Начинает выполнение нового теста
        /// </summary>
        /// <param name="phone">Номер телефона</param>
        [HttpGet]
        [AllowAnonymous]
        public ActionResult CPTestBegin(string phone)
        {
            try
            {
                var test = db.CallProjectTestNumbers.FirstOrDefault(e => e.Phone == phone)?.CallProjectTest;

                if (test == null) return new HttpStatusCodeResult(HttpStatusCode.NotFound);

                var entity = new CallProjectTestResult
                {
                    CallProjectTest_Id = test.Id,
                    Phone = phone,
                    CreationDate = DateTime.Now,
                    CompleteDate = null
                };

                db.CallProjectTestResults.Add(entity);
                db.SaveChanges();

                return new HttpStatusCodeResult(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                return new HttpStatusCodeResult(HttpStatusCode.InternalServerError, ex.Message);
            }
        }

        /// <summary>
        /// Сохраняет результат шага теста
        /// </summary>
        /// <param name="phone">Номер телефона</param>
        /// <param name="index">Порядковый номер</param>
        /// <param name="wcr">WCR</param>
        /// <param name="wer">WER</param>
        [HttpGet]
        [AllowAnonymous]
        public ActionResult CPTestStep(string phone, int index, int wcr, int wer)
        {
            try
            {
                var result = db.CallProjectTestResults.LastOrDefault(e => e.Phone == phone && e.CompleteDate == null);

                if (result == null) return new HttpStatusCodeResult(HttpStatusCode.NotFound);

                var entity = new CallProjectTestStepResult
                {
                    CallProjectTestResult_Id = result.Id,
                    CreationDate = DateTime.Now,
                    WCR = wcr,
                    WER = wer
                };

                db.CallProjectTestStepResults.Add(entity);
                db.SaveChanges();

                return new HttpStatusCodeResult(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                return new HttpStatusCodeResult(HttpStatusCode.InternalServerError, ex.Message);
            }
        }

        /// <summary>
        /// Завершает выполнение теста
        /// </summary>
        /// <param name="phone">Номер телефона</param>
        [HttpGet]
        [AllowAnonymous]
        public ActionResult CPTestEnd(string phone)
        {
            try
            {
                var result = db.CallProjectTestResults.LastOrDefault(e => e.Phone == phone && e.CompleteDate == null);

                if (result == null) return new HttpStatusCodeResult(HttpStatusCode.NotFound);

                result.CompleteDate = DateTime.Now;

                db.SaveChanges();

                var number = db.CallProjectTestNumbers.FirstOrDefault(e => e.Phone == phone);
                if (number != null)
                {
                    number.State = "N";
                    number.LastAttemptDate = DateTime.Now;
                    db.SaveChanges();
                }

                return new HttpStatusCodeResult(HttpStatusCode.OK);
            }
            catch (Exception ex)
            {
                return new HttpStatusCodeResult(HttpStatusCode.InternalServerError, ex.Message);
            }
        }

        #endregion

        #region .. AnalystJobs ..
        public static bool AnalystJobsProcessing = false;

        public void AnalyzeJobs()
        {
            NLog.Fluent.Log.Trace().Message("Запуск AnalyzeJobs: " + DateTime.Now).Write();
            try
            {
                if (!AnalystJobsProcessing) 
                    try
                    {
                        AnalystJobsProcessing = true;

                        var callJobs = db.CallJobs
                            .Where(e => e.IsActive)
                            .Where(e => e.TimeStart <= DbFunctions.CreateTime(DateTime.Now.Hour-1, DateTime.Now.Minute, DateTime.Now.Second) && e.TimeStop > DbFunctions.CreateTime(DateTime.Now.Hour, DateTime.Now.Minute, DateTime.Now.Second))
                            .Where(e => e.Type == CallJobType.OutboundProgressive)
                            .Where(e => e.IsEnableAnalytics)
                            .Distinct()
                            .ToList();

                        var callProjects = callJobs.Select(e => e.CallProject).Distinct().ToList();

                        foreach(var project in callProjects)
                        {
                            AnalyzeProject(project);
                        }

                        foreach (var callJob in callJobs)
                        {
                            NLog.Fluent.Log.Trace().Message("Запуск AnalyzeJobs по задаче: " + callJob.Name + " " + DateTime.Now).Write();
                            if(callJob.IsHB)
                            {
                                //if(callJob.CallsMore12sec == 0 || callJob.CallsMore60sec == 0) { continue; }
                                AnalyzeHBJob(callJob);
                            }
                            else
                            {
                                //if (callJob.CallEfforts == 0 || callJob.PercentConnections == 0 || callJob.OperatorJoinAmount == 0 || callJob.OperatorCallsMore60sec == 0) { continue; }
                                AnalyzeRobotJob(callJob);
                            }
                        }
                    }
                    finally { AnalystJobsProcessing = false; }
            }
            catch (Exception ex)
            {
                NLog.Fluent.Log.Error().Exception(ex).Write();
            }
        }

        public async void AnalyzeProject(CallProject project)
        {
            var numbers = db.Database.SqlQuery<long>($@"select	count(0)
				                                    from {db.DbName()}.dbo.CallBaseNumbers_{project.Id}
                                                    ").First();

            var maxNumbers = db.Options.FirstOrDefault(e => e.Name == "Проект дозвона / максимальное количество оперативных номеров /").Value;

            if (numbers >= Convert.ToInt32(maxNumbers))
            {
                var stringTitle = "Превышение оперативных номеров на пректе " + project.Name;
                var stringMsg = "Оперативных номеров: " + numbers +
                    " <br> Максимальное кол-во оперативных номеров: " + maxNumbers;

                await SendOkdeskIssueAsync(stringTitle, stringMsg);
            }
        }

        public async void AnalyzeHBJob(CallJob job)
        {
            var date = DateTime.Now;

            var operatorsPerHour = db.Database.SqlQuery<decimal>($@"
                        with 
	                        osh (RowNum, Operator_Id, State, DateTime)
	                        as (
		                        select 
			                        ROW_NUMBER() over (partition by osh.Operator_Id order by osh.Id) RowNum,
			                        osh.Operator_Id Operator_Id,
			                        osh.State State,
			                        osh.CreationDate DateTime
		                        from
			                        SelenaCRM.dbo.OperatorStateHistories osh,
			                        SelenaCRM.dbo.CallProjects cp,
			                        SelenaCRM.dbo.Franchisees f
		                        where
			                        osh.CallProject_Id = cp.Id and
			                        cp.Franchisee_Id = f.Id and
			                        osh.CallProject_Id = @ProjectId and
			                        dateadd(minute, f.TimeOffset - @ServerOffset, osh.CreationDate) between dateadd(day, datediff(day, 0, @Date), 0) and dateadd(day, datediff(day, 0, @Date) + 1, 0)
	                        )

                        select sum (datediff(second, case when dateadd(minute, f.TimeOffset - @ServerOffset, osh1.DateTime) > DATEADD(hour,-1,@Date) 
				                        then dateadd(minute, f.TimeOffset - @ServerOffset, osh1.DateTime) else DATEADD(hour,-1,@Date) end,
			                        case when osh2.DateTime is null or dateadd(minute, f.TimeOffset - @ServerOffset, osh2.DateTime) > dateadd(hour, 1, DATEADD(hour,-1,@Date)) 
				                        then case when dateadd(hour, 1, DATEADD(hour,-1,@Date)) > dateadd(minute, f.TimeOffset - @ServerOffset, @Date) then dateadd(minute, f.TimeOffset - @ServerOffset, getdate()) else dateadd(hour, 1, DATEADD(hour,-1,@Date)) end else dateadd(minute, f.TimeOffset - @ServerOffset, osh2.DateTime) end)) / 3600.0 OpHour
	                        from
		                        osh osh1 left outer join
		                        osh osh2 on osh1.Operator_Id = osh2.Operator_Id and osh1.RowNum = osh2.RowNum - 1,
		                        SelenaCRM.dbo.Operators o,
		                        SelenaCRM.dbo.CallProjects p,
		                        SelenaCRM.dbo.Franchisees f,
		                        SelenaCRM.dbo.OperatorCallJobs ocj
	                        where
		                        osh1.Operator_Id = o.Id and
		                        ocj.Operator_Id = o.Id and
		                        ocj.CallJob_Id = @CallJob_Id and
		                        p.Franchisee_Id = f.Id and
		                        p.Id = @ProjectId and
		                        osh1.State = 1 and
		                        DATEADD(hour,-1,@Date) < dateadd(minute, f.TimeOffset - @ServerOffset, @Date) and
		                        dateadd(minute, f.TimeOffset - @ServerOffset, osh1.DateTime) < dateadd(hour, 1, DATEADD(hour,-1,@Date)) and (osh2.DateTime is null or dateadd(minute, f.TimeOffset - @ServerOffset, osh2.DateTime) > DATEADD(hour,-1,@Date))
	                        group by
		                        o.CallCenter_Id
					",
            new SqlParameter("@CallJob_Id", job.Id),
            new SqlParameter("@Date", date),
            new SqlParameter("@ProjectId", job.CallProject_Id),
            new SqlParameter("@ServerOffset", 180)).FirstOrDefault();

            var callsMore12 = db.Database.SqlQuery<int>($@"
					    SELECT count(*)
                      FROM [SelenaCRM].[dbo].[OperatorCalls]
                      where TimeStart between DATEADD(HOUR,-1,@Date) and @date
                      and CallJob_Id = @CallJob
                      and Duration >= 12
					",
            new SqlParameter("@CallJob", job.Id),
            new SqlParameter("@Date", date)).FirstOrDefault();

            var callsMore60 = db.Database.SqlQuery<int>($@"
					    SELECT count(*)
                      FROM [SelenaCRM].[dbo].[OperatorCalls]
                      where TimeStart between DATEADD(HOUR,-1,@Date) and @Date
                      and CallJob_Id = @CallJob
                      and Duration >= 60
					",
            new SqlParameter("@CallJob", job.Id),
            new SqlParameter("@Date", date)).FirstOrDefault();

            var callsMore12PerOpHour = callsMore12 / operatorsPerHour;
            var callsMore60PerOpHour = callsMore60 / operatorsPerHour;

            NLog.Fluent.Log.Trace().Message(job.Name + " / " + callsMore12PerOpHour + " / " + callsMore60PerOpHour).Write();
            
            if((double)operatorsPerHour > 0.5)
            {
                if (callsMore12PerOpHour < job.CallsMore12sec || callsMore60PerOpHour < job.CallsMore60sec)
                {
                    var stringTitle = "Низкие показатели задачи " + job.Name;
                    var stringMsg = "Низкие показатели задачи: " + job.Name + "<br> Время: " + date.AddHours(-1) + " - " + date +
                        " <br>     Количество коммутаций более 12 сек на 1 оператор/час: " + callsMore12PerOpHour.ToString("0.##") + " необходимо " + job.CallsMore12sec +
                        " <br>     Количество коммутаций более 60 сек на 1 оператор/час: " + callsMore60PerOpHour.ToString("0.##") + " необходимо " + job.CallsMore60sec;

                    NLog.Fluent.Log.Trace().Message(stringMsg).Write();
                    await SendOkdeskIssueAsync(stringTitle, stringMsg);
                }
            }
            
        }

        public async void AnalyzeRobotJob(CallJob job)
        {
            var date = DateTime.Now;

            var operatorsPerHour = db.Database.SqlQuery<decimal>($@"
                        with 
	                        osh (RowNum, Operator_Id, State, DateTime)
	                        as (
		                        select 
			                        ROW_NUMBER() over (partition by osh.Operator_Id order by osh.Id) RowNum,
			                        osh.Operator_Id Operator_Id,
			                        osh.State State,
			                        osh.CreationDate DateTime
		                        from
			                        SelenaCRM.dbo.OperatorStateHistories osh,
			                        SelenaCRM.dbo.CallProjects cp,
			                        SelenaCRM.dbo.Franchisees f
		                        where
			                        osh.CallProject_Id = cp.Id and
			                        cp.Franchisee_Id = f.Id and
			                        osh.CallProject_Id = @ProjectId and
			                        dateadd(minute, f.TimeOffset - @ServerOffset, osh.CreationDate) between dateadd(day, datediff(day, 0, @Date), 0) and dateadd(day, datediff(day, 0, @Date) + 1, 0)
	                        )

                        select sum (datediff(second, case when dateadd(minute, f.TimeOffset - @ServerOffset, osh1.DateTime) > DATEADD(hour,-1,@Date) 
				                        then dateadd(minute, f.TimeOffset - @ServerOffset, osh1.DateTime) else DATEADD(hour,-1,@Date) end,
			                        case when osh2.DateTime is null or dateadd(minute, f.TimeOffset - @ServerOffset, osh2.DateTime) > dateadd(hour, 1, DATEADD(hour,-1,@Date)) 
				                        then case when dateadd(hour, 1, DATEADD(hour,-1,@Date)) > dateadd(minute, f.TimeOffset - @ServerOffset, @Date) then dateadd(minute, f.TimeOffset - @ServerOffset, getdate()) else dateadd(hour, 1, DATEADD(hour,-1,@Date)) end else dateadd(minute, f.TimeOffset - @ServerOffset, osh2.DateTime) end)) / 3600.0 OpHour
	                        from
		                        osh osh1 left outer join
		                        osh osh2 on osh1.Operator_Id = osh2.Operator_Id and osh1.RowNum = osh2.RowNum - 1,
		                        SelenaCRM.dbo.Operators o,
		                        SelenaCRM.dbo.CallProjects p,
		                        SelenaCRM.dbo.Franchisees f,
		                        SelenaCRM.dbo.OperatorCallJobs ocj
	                        where
		                        osh1.Operator_Id = o.Id and
		                        ocj.Operator_Id = o.Id and
		                        ocj.CallJob_Id = @CallJob_Id and
		                        p.Franchisee_Id = f.Id and
		                        p.Id = @ProjectId and
		                        osh1.State = 1 and
		                        DATEADD(hour,-1,@Date) < dateadd(minute, f.TimeOffset - @ServerOffset, @Date) and
		                        dateadd(minute, f.TimeOffset - @ServerOffset, osh1.DateTime) < dateadd(hour, 1, DATEADD(hour,-1,@Date)) and (osh2.DateTime is null or dateadd(minute, f.TimeOffset - @ServerOffset, osh2.DateTime) > DATEADD(hour,-1,@Date))
	                        group by
		                        o.CallCenter_Id
					",
            new SqlParameter("@CallJob_Id", job.Id),
            new SqlParameter("@Date", date),
            new SqlParameter("@ProjectId", job.CallProject_Id),
            new SqlParameter("@ServerOffset", 180)).FirstOrDefault();

            var lines = db.Database.SqlQuery<int>($@"
					    SELECT sum(csls.TotalCount)
                          FROM [SelenaCRM].[dbo].[CallServerLineStates] csls
                          join (Select distinct Prefix from [SelenaCRM].[dbo].[CallProjectRoutes] where CallProject_Id = @CallProject) cpr on csls.Prefix = cpr.Prefix
                          where csls.Prefix not like '%-in%'
					",
            new SqlParameter("@CallProject", job.CallProject_Id)).FirstOrDefault();

            var callEfforts = db.Database.SqlQuery<int>($@"
					    SELECT count(*)
                          FROM [SelenaSTOR].[dbo].[CallEfforts]
                          where TimeStart between DATEADD(HOUR,-1,@Date) and @Date
                          and CallJob_Id = @CallJob
					",
            new SqlParameter("@CallJob", job.Id),
            new SqlParameter("@Date", date)).FirstOrDefault();

            var callConnections = db.Database.SqlQuery<int>($@"
					    SELECT count(*)
                          FROM [SelenaSTOR].[dbo].[CallEfforts] ce
                          inner join [SelenaSTOR].[dbo].[CallConnections] cc on ce.Id = cc.CallEffort_Id
                          where ce.TimeStart between DATEADD(HOUR,-1,@Date) and @Date
                          and ce.CallJob_Id = @CallJob
					",
            new SqlParameter("@CallJob", job.Id),
            new SqlParameter("@Date", date)).FirstOrDefault();

            var operatorJoins = db.Database.SqlQuery<int>($@"
					    SELECT count(*)
                          FROM [SelenaSTOR].[dbo].[CallEfforts] ce
                          inner join [SelenaSTOR].[dbo].[CallConnections] cc on ce.Id = cc.CallEffort_Id
                          where ce.TimeStart between DATEADD(HOUR,-1,@Date) and @Date
                          and ce.CallJob_Id = @CallJob
                          and cc.TimeJoin is not null
					",
            new SqlParameter("@CallJob", job.Id),
            new SqlParameter("@Date", date)).FirstOrDefault();

            var callsMore60 = db.Database.SqlQuery<int>($@"
					    SELECT count(*)
                      FROM [SelenaCRM].[dbo].[OperatorCalls]
                      where TimeStart between DATEADD(HOUR,-1,@Date) and @Date
                      and CallJob_Id = @CallJob
                      and Duration >= 60
					",
            new SqlParameter("@CallJob", job.Id),
            new SqlParameter("@Date", date)).FirstOrDefault();

            var callEffortsPerLine = callEfforts / (Convert.ToDouble(lines));
            var percentConnections = (Convert.ToDouble(callConnections) / Convert.ToDouble(callEfforts)) * 100;
            var operatorJoinsPerOpHour50lines = operatorJoins / Convert.ToDouble(operatorsPerHour) / (lines / 50.0f);
            var callsMore60PerOpHour50lines = callsMore60 / Convert.ToDouble(operatorsPerHour) / (lines / 50.0f);

            if((double)operatorsPerHour > 0.5)
            {
                if (callEffortsPerLine < job.CallEfforts || percentConnections < job.PercentConnections ||
                operatorJoinsPerOpHour50lines < job.OperatorJoinAmount || callsMore60PerOpHour50lines < job.OperatorCallsMore60sec)
                {
                    var stringTitle = "Низкие показатели задачи " + job.Name;
                    var stringMsg = "Низкие показатели задачи: " + job.Name + " <br> Время: " + date.AddHours(-1) + " - " + date +
                        " <br>     Дозвонов на 1 линию/час: " + callEffortsPerLine.ToString("0.##") + " необходимо " + job.CallEfforts +
                        " <br>     Процент коммутаций с абонентом: " + percentConnections.ToString("0.##") + " необходимо " + job.PercentConnections +
                        " <br>     Переводов на оператора на оператор/час на 50 линий: " + operatorJoinsPerOpHour50lines.ToString("0.##") + " необходимо " + job.OperatorJoinAmount +
                        " <br>     Разговоров с оператором более 1 мин на оператор/час на 50 линий: " + callsMore60PerOpHour50lines.ToString("0.##") + " необходимо " + job.OperatorCallsMore60sec;

                    NLog.Fluent.Log.Trace().Message(stringMsg).Write();
                    await SendOkdeskIssueAsync(stringTitle, stringMsg);
                }
            }
        }

        public class Issue
        {
            public string title { get; set; }
            public string description { get; set; }
            public string group_id { get; set; }
        }

        public class OkdeskMsg
        {
            public Issue issue { get; set; }
        }

        public async Task SendOkdeskIssueAsync(string strTitle, string strMsg)
        {
            NLog.Fluent.Log.Trace().Message($"попытка отправки заявки в okdesk:  {strTitle}").Write();
            try
            {
                var issueMsg = new Issue
                {
                    title = strTitle,
                    description = strMsg,
                    group_id = "1"
                };

                var okdesk = new OkdeskMsg
                {
                    issue = issueMsg
                };

                var bodyMsg = JsonConvert.SerializeObject(okdesk);

                NLog.Fluent.Log.Trace().Message($"тело заявки в okdesk:  {bodyMsg}").Write();

                HttpClient httpClient = new HttpClient();
                string url = "https://l4l.okdesk.ru/api/v1/issues/?api_token=a796b1954ab040bb9edb1e683ef6cd3ece90c050";

                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

                var response = httpClient.PostAsync(new Uri(url), new StringContent(bodyMsg, Encoding.UTF8, "application/json")).Result;
                string responseBody = await response.Content.ReadAsStringAsync();
                var responseDict = JsonConvert.DeserializeObject<Dictionary<string, string>>(responseBody);
                if (responseDict.ElementAt(0).Key == "id")//responseDict.ContainsKey("id"))
                {
                    NLog.Fluent.Log.Trace().Message($"отправлена заявка в okdesk:  {strTitle}").Write();
                }
                else
                {
                    NLog.Fluent.Log.Trace().Message($"Ошибка!!! заявка в okdesk: {strTitle} не отправлена {responseBody}").Write();
                }

            }
            catch (Exception ex)
            {
                NLog.Fluent.Log.Trace().Message($"Ошибка!!! заявка в okdesk: {strTitle} не отправлена: {ex.Message}").Write();
            }
        }

        #endregion

        /*#region .. WEB ..

        //[HttpGet]
        //[AllowAnonymous]
        //public ContentResult CreateCallConnection(string from, string gate, string calledId, string callerId, DateTime timeStart, DateTime timeAnswer, DateTime? timeTransfer, DateTime timeStop)
        //{
        //    try
        //    {
        //        var server = db.CallServers.FirstOrDefault(e => e.Code.ToLower() == from.ToLower());

        //        storage.CallConnections.Add(new Storage.CallConnection
        //        {
        //            CallServer_Id = server.Id,
        //            Gate = gate,
        //            CalledId = calledId,
        //            CallerId = callerId,
        //            TimeStart = timeStart,
        //            TimeAnswer = timeAnswer,
        //            TimeTransfer = timeTransfer,
        //            TimeStop = timeStop
        //        });

        //        storage.SaveChanges();

        //        return Content("OK");
        //    }
        //    catch (Exception ex)
        //    {
        //        return Content(ex.Message);
        //    }
        //}

        //[HttpGet]
        //[AllowAnonymous]
        //public ContentResult LineAnswer(string from, string line, string session)
        //{
        //    NLog.Fluent.Log.Trace().Message($"from: {from} / line: {line} / session: {session}").Write();

        //    //var server = db.CallServers.FirstOrDefault(e => e.Code.ToLower() == from.ToLower());
        //    //var dial = db.CallServerDialStates.FirstOrDefault(e => e.CallServer_Id == server.Id && e.Name == line);

        //    //if (dial == null) db.CallServerDialStates.Add(new CallServerDialState { CallServer_Id = server.Id, Name = line, SessionId = new Guid(session), TimeAnswer = DateTime.Now });
        //    //else { dial.SessionId = new Guid(session); dial.TimeAnswer = DateTime.Now; }

        //    //db.SaveChanges();

        //    return Content("OK");
        //}

        //[HttpGet]
        //[AllowAnonymous]
        //public ContentResult LineHangUp (string from, string line, string session)
        //{
        //    NLog.Fluent.Log.Trace().Message($"from: {from} / line: {line} / session: {session}").Write();

        //    //var server = db.CallServers.FirstOrDefault(e => e.Code.ToLower() == from.ToLower());
        //    //var dial = db.CallServerDialStates.FirstOrDefault(e => e.CallServer_Id == server.Id && e.Name == line);

        //    //if (dial == null) db.CallServerDialStates.Add(new CallServerDialState { CallServer_Id = server.Id, Name = line, SessionId = null, TimeAnswer = null });
        //    //else { dial.SessionId = null; dial.TimeAnswer = null; }

        //    //db.SaveChanges();

        //    return Content("OK");
        //}

        [HttpGet]
        [AllowAnonymous]
        public ContentResult UpdateLineStates(string from, string code, int totalCount, int callCount, int dialCount, int readyCount)
        {
            try
            {
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

                db.Database.ExecuteSqlCommand($"delete {db.DbName()}.dbo.CallServerLineStates where LastAssignDate is null or LastAssignDate < @LastAssignDate", new SqlParameter("@LastAssignDate", DateTime.Now.AddSeconds(-10)));

                return Content("OK");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpGet]
        [AllowAnonymous]
        public ContentResult CreateOperatorCall(long callProject_Id, long callBaseNumber_Id, string connection_Id, string gate)
        {
            try
            {
                var operatorCall_Id = db.Database.SqlQuery<long>($@"
                    insert	{db.DbName()}.dbo.OperatorCalls(Operator_Id, OktellConnectionId, Client_Id, CallProject_Id, CallBase_Id, TimeStart, Duration, DialMethod, DialStatus, CallBaseNumber, Confirmed, AbonentAnswerTime, Gate)
                    output	inserted.Id
                    select	null, @OktellConnectionId, null, @CallProject_Id, CallBase_Id, getDate(), null, {(int)CallDialMethod.Auto}, {(int)CallDialStatus.Success}, Phone, 0, 0, @Gate
                    from	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id} where Id = @Id
                    ",
                    new SqlParameter("@Id", callBaseNumber_Id),
                    new SqlParameter("@CallProject_Id", callProject_Id),
                    new SqlParameter("@OktellConnectionId", string.IsNullOrEmpty(connection_Id) ? DBNull.Value : (object)Guid.Parse(connection_Id)),
                    new SqlParameter("@Gate", gate ?? string.Empty)).First();

                return Content(operatorCall_Id.ToString());
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        [HttpGet]
        [AllowAnonymous]
        public ContentResult CreateCallbackEffort(long callProject_Id, string phone)
        {
            try
            {
                var callEffort_Id = db.Database.SqlQuery<long>($@"
					insert	{storage.DbName()}.dbo.CallEfforts(CallProject_Id, CallBase_Id, TimeStart, DialMethod, CallBaseNumber)
					output	inserted.Id
                    values  (@CallProject_Id, null, getDate(), @DialMethod, @CallBaseNumber)
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

        //[HttpGet]
        //[AllowAnonymous]
        //public ContentResult CreateCallbackCall(long callProject_Id, string phone, string connection_Id, string gate)
        //{
        //    try
        //    {
        //        var operatorCall_Id = db.Database.SqlQuery<long>($@"
        //            insert	{db.DbName()}.dbo.OperatorCalls(Operator_Id, OktellConnectionId, Client_Id, CallProject_Id, CallBase_Id, TimeStart, Duration, DialMethod, DialStatus, CallBaseNumber, Confirmed, AbonentAnswerTime, Gate)
        //            output	inserted.Id
        //            select	null, @OktellConnectionId, null, @CallProject_Id, CallBase_Id, getDate(), null, {(int)CallDialMethod.Auto}, {(int)CallDialStatus.Success}, Phone, 0, 0, @Gate
        //            from	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id} where Id = @Id
        //            ",
        //            new SqlParameter("@Id", callBaseNumber_Id),
        //            new SqlParameter("@CallProject_Id", callProject_Id),
        //            new SqlParameter("@OktellConnectionId", string.IsNullOrEmpty(connection_Id) ? DBNull.Value : (object)Guid.Parse(connection_Id)),
        //            new SqlParameter("@Gate", gate ?? string.Empty)).First();

        //        return Content(operatorCall_Id.ToString());
        //    }
        //    catch (Exception ex)
        //    {
        //        return Content(ex.Message);
        //    }
        //}


        [HttpGet]
        [AllowAnonymous]
        public ContentResult UpdateNumberState(long callProject_Id, long callBaseNumber_Id, string state)
        {
            try
            {
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
							set     State = case when AttemptsQty < @AttemptsQty then 'T' else 'C' end,
									Result = 0,
                                    LastAttemptDate = getDate()
							where   Id = @id
							",
                            new SqlParameter("@id", callBaseNumber_Id),
                            new SqlParameter("@AttemptsQty", db.CallProjects.AsNoTracking().First(e => e.Id == callProject_Id).DialEffortLimit));
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

                    case "D":
                        db.Database.ExecuteSqlCommand($@"
							update	{db.DbName()}.dbo.CallBaseNumbers_{callProject_Id}
							set     State = 'D',
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

							insert	{storage.DbName()}.dbo.CallEfforts(CallProject_Id, CallBase_Id, TimeStart, DialMethod, CallBaseNumber)
							output	inserted.Id
							select	{callProject_Id}, CallBase_Id, getDate(), {(int)CallDialMethod.Auto}, Phone
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

        [HttpGet]
        [AllowAnonymous]
        public ContentResult UpdateCheckBaseNumberState(long checkProject_Id, long checkBaseNumber_Id, string state)
        {
            try
            {
                switch (state.ToUpper())
                {
                    case "L":
                        db.Database.ExecuteSqlCommand($@"
                            update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
                            set     Status = null
                            where   Id = @id
                            ", new SqlParameter("@id", checkBaseNumber_Id));
                        break;

                    case "P":
                        db.Database.ExecuteSqlCommand($@"
                            update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
                            set     Status = 'Процессинг'
                            where   Id = @id
                            ", new SqlParameter("@id", checkBaseNumber_Id));
                        break;

                    case "C":
                        db.Database.ExecuteSqlCommand($@"
                            update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
                            set     Status = 'Ответ'
                            where   Id = @id
                            ", new SqlParameter("@id", checkBaseNumber_Id));
                        break;

                    case "F":
                        db.Database.ExecuteSqlCommand($@"
                            update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
                            set     Status = 'БыстрОтв'
                            where   Id = @id
                            ", new SqlParameter("@id", checkBaseNumber_Id));
                        break;

                    case "T":
                        db.Database.ExecuteSqlCommand($@"
                            update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
                            set     Status = 'Не отвечает'
                            where   Id = @id
                            ", new SqlParameter("@id", checkBaseNumber_Id));
                        break;

                    case "B":
                        db.Database.ExecuteSqlCommand($@"
                            update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
                            set     Status = 'Занято'
                            where   Id = @id
                            ", new SqlParameter("@id", checkBaseNumber_Id));
                        break;

                    case "E":
                        db.Database.ExecuteSqlCommand($@"
                            update	{db.DbName()}.dbo.CheckBaseNumbers_{checkProject_Id}
                            set     Status = 'Ошибка'
                            where   Id = @id
                            ", new SqlParameter("@id", checkBaseNumber_Id));
                        break;
                }
                return Content("OK");
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }

        #endregion*/

        public ActionResult Read([DataSourceRequest]DataSourceRequest request)
        {
            LogEvent(Operation.Read);
            if (!CheckIP()) return ModelStateResult();
            if (!CheckTimeOffset()) return ModelStateResult();
            if (!Check(Operation.Read)) return ModelStateResult();

            var callServers = db.CallServers
                .ToList()
                .Select(e => new
                {
                    Id = e.Id,
                    HashCode = e.HashCode(),
                    Name = e.Name,
                    Code = e.Code,
                    URL = e.URL,
                    IsActive = e.IsActive,
                    Lines = string.Join(",", e.CallServerLineStates.Select(line => line.Provider.Code).ToList())
                });

            DataSourceResult result = callServers.ToDataSourceResult(request, entity => new CallServerViewModel
            {
                Id = entity.Id,
                HashCode = entity.HashCode,
                Name = entity.Name,
                Code = entity.Code,
                URL = entity.URL,
                IsActive = entity.IsActive,
                Lines = entity.Lines
            });

            return Json(result);
        }

        [AcceptVerbs(HttpVerbs.Post)]
        public ActionResult Create([DataSourceRequest]DataSourceRequest request, CallServerViewModel model)
        {
            CheckIP(); LogEvent(Operation.Create);

            if (ModelState.IsValid && Check(Operation.Create))
            {
                var entity = new CallServer
                {
                    URL = model.URL.Trim(),
                    Name = model.Name.NullifyTrim(),
                    Code = model.Code.Trim(),
                    IsActive = model.IsActive
                };

                db.CallServers.Add(entity);
                db.SaveChanges();

                model.Id = entity.Id;
                model.HashCode = entity.HashCode();

                Log(Operation.Create, null, model.Id, model);
            }

            return Json(new[] { model }.ToDataSourceResult(request, ModelState));
        }

        [AcceptVerbs(HttpVerbs.Post)]
        public ActionResult Update([DataSourceRequest]DataSourceRequest request, CallServerViewModel model)
        {
            CheckIP(); LogEvent(Operation.Update);

            if (ModelState.IsValid && CheckHash(model) && Check(Operation.Update))
            {
                var entity = new CallServer
                {
                    Id = model.Id,
                    URL = model.URL.Trim(),
                    Name = model.Name.NullifyTrim(),
                    Code = model.Code.Trim(),
                    IsActive = model.IsActive
                };

                db.CallServers.Attach(entity);
                db.Entry(entity).State = EntityState.Modified;
                db.SaveChanges();

                model.HashCode = entity.HashCode();

                Log(Operation.Update, null, model.Id, model);
            }

            return Json(new[] { model }.ToDataSourceResult(request, ModelState));
        }

        [AcceptVerbs(HttpVerbs.Post)]
        public ActionResult Destroy([DataSourceRequest]DataSourceRequest request, CallServerViewModel model)
        {
            CheckIP(); LogEvent(Operation.Destroy);

            if (ModelState.IsValid && Check(Operation.Destroy))
            {
                var entity = new CallServer
                {
                    Id = model.Id
                };

                db.CallServers.Attach(entity);
                db.CallServers.Remove(entity);
                db.SaveChanges();

                Log(Operation.Destroy, null, model.Id, model);
            }

            return Json(new[] { model }.ToDataSourceResult(request, ModelState));
        }

        public override IEnumerable<DataSourceEntry> AsDataSource()
        {
            return db.CallServers
                .Select(e => new DataSourceEntry
                {
                    Id = e.Id,
                    Name = e.URL,
                    Group = string.Empty
                })
                .OrderBy(e => e.Name)
                .ToList();
        }

        [HttpGet]
        [AllowAnonymous]
        public ActionResult AddClient(string Email, string Password, long Franchisee_Id, string Name, string Phone, long ClientService_Id, int? Age, int? Height, int? Weight, string Description, long? Clinic_Id)
        {
            try
            {
                var user = UserManager.FindByEmail(Email);
                var oper = db.Operators.AsNoTracking().FirstOrDefault(e => e.Email.ToLower() == Email.ToLower() && e.User_Id.HasValue);

                if (oper != null) Password = "op_" + Password;

                var result = UserManager.CheckPassword(user, Password) ? SignInStatus.Success : SignInStatus.Failure;

                if (result == SignInStatus.Success) result = UserManager.GetLockoutEndDate(user.Id) < DateTimeOffset.Now ? SignInStatus.Success : SignInStatus.LockedOut;

                if (result == SignInStatus.Success)
                {


                    string LastName = "";
                    string FirstName = "";
                    string Patronymic = "";
                    try
                    {
                        string[] words = Name.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        LastName = words[0];
                        FirstName = words[1];
                        Patronymic = words[2];
                    }
                    catch (Exception)
                    {

                    }
                    var client = db.Clients.Add(new Client
                    {
                        Franchisee_Id = Franchisee_Id,
                        Clinic_Id = Clinic_Id,
                        Patronymic = Patronymic,
                        LastName = LastName,
                        FirstName = FirstName,
                        Name = Name,
                        Phone = Phone,
                        CreationDate = DateTime.Now,
                        Status = ClientStatus.New,
                        Valid = false,
                        SmsBan = false,
                        ClientService_Id = ClientService_Id
                    });
                    db.SaveChanges();

                    db.ClientStatusHistories.Add(new ClientStatusHistory
                    {
                        Status = ClientStatus.New,
                        CreationDate = DateTime.Now,
                        Client_Id = client.Id
                    });
                    db.SaveChanges();

                    return Content("Ok");

                }
                else
                {
                    return Content("В доступе отказано");
                }
            }
            catch (Exception ex)
            {
                return Content(ex.Message);
            }
        }
    }
}