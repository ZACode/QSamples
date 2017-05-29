/ RM
.cfg.nodes:flip `node`hostname`ipaddress`tipe`port`region`ds`rack`amem`acpu`almem`alcpu`status!()
.cfg.topics:`id`name`rf`region`ds`crtime`crby`msgpday`sttime`entime!/:enlist (1;`tradetopic;3;`amrs;`scs;.z.p;`admin;1000000;00:00:00.000;23:59:59.000)
.cfg.dir.work
.cfg.dir.tmp
.cfg.dir.log
.cfg.dir.slog
.cfg.dir.slname
.cfg.sysuser:.z.u;

.cfg.proc.tipe:exec tipe[0] from node where host=.z.h, port=.z.P;


startNode:{cmd:"ssh ",x," \"cd ",.cfg.dir.work," ; q -p ",y," </dev/null>2&1>>",.cfg.dir.slog,"/",.cfg.dir.slname," &\"";
@[system;cmd;{log `err x}];
}

/start borkers
startNode each exec !'[-1;`$ip,'":",'port] from .cfg.nodes where tipe = `broker, status=`down

/start forward services
startNode each exec !'[-1;`$ip,'":",'port] from .cfg.nodes where tipe = `forwarder, status=`down

sendLibs:{


/sendlib needs to be checked

.str.init:{t:(exec distinct name from .cfg.topics);.str.subs:t!(count t)#();}

.str.datain:{[t;d] d:.z.p,'$[0h~type first d;d;enlist d];
 .str.pub[t;d] };

.str.sub:{ .str.addsub[;y] each $[x~`;key .str.subs;x]};
.str.init[]
.str.subs[`tradetopic;0;1]
.str.addsub:{ 
   $[(count .str.subs[x])<i:.str.subs[x;;0]?.z.w;
  / .[`.str.subs;(x;i;1);union;y];
   .str.subs[x;i;1],:y;
 .str.subs[x],:enlist(.z.w;y) / no restriction on topic list
  ];};

.str.delsub:{.str.subs[x]_:.str.subs[x;;0]?y};

.z.pc:{if[x in raze value .str.subs[;;0]; .str.delsub[;x] each key .str.subs];update et:.z.p from `.cfg.sysconn where h=x, et=0Np;}

.str.pub:{if[not x in key .str.subs;:()];
 {(neg z)(`datain;x;y)}[x;y;] each .str.subs[x;;0]; }
 

/ system init
.cfg.proc.tipe:`broker;



/ connection lib
.cfg.sysconn:flip `host`ipa`h`st`et!()
.cfg.sysuser:`admin;
sysconnect:{
 h:.z.h;
 ip:.z.h;
 u:`admin;
 $[(.cfg.proc.tipe=`broker)|
 (0<count exec i from .cfg.nodes where hostname=h, ipaddress=ip ); / , u=.cfg.sysuser);
 [connupdate[h;ip];:1b]; 0b]
}
sysconnect[]
connupdate:{[h;ip] insert[`.cfg.sysconn;(h;ip;.z.w;.z.p;0Np)];}

.z.po:{sysconnect[];}
.z.pc:{update et:.z.p from `.cfg.sysconn where h=.z.w,et=0Np;}
/
