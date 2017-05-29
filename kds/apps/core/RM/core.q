/ RM
.cfg.nodes:`node`hostname`ipaddress`tipe`port`region`ds`rack`amem`acpu`almem`alcpu`status!()
.cfg.topics.`id`name`rf`region`ds`crtime`crby`msgpday`sttime`entime!()
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


/
init:{.stream.subs:t!(count t)#t:(exec distinct name from .cfg.topics)}

.stream.datain:{[t;d] d:.z.p,'$[0h~type first d;d;enlist d];
 pub[t;d]
};

sub:{ addsub[;y] each $[x~`;key .stream.subs;x]};

addsub:{ 
 $[(count .stream.subs)>i:.stream.subs[x;;0]?.z.w;
  .[`.stream.subs;(x;i;1);union;y]; 
 .stream.subs[x]:enlist(.z.w;y] / no restriction on topic list
  ];};

delsub:{.stream.subs[x]_:.stream.subs[x;;0]?.z.w};
.z.pc:{if[.z.w in raze .stream.subs[;;0]; delsub each key .stream.subs;
update et:.z.p from `cfg.sysconn where host=h;h=.z.w;et=0Np;}

pub:{if[not x in key .stream.subs;:()];
 {(neg z)(`datain;x;y)}[x;y;] each .stream.subs[x;;0]; }
 
/

/ system init
.cfg.proc.tipe:



/ connection lib
.cfg.sysconn:`host`ipa`h`st`et!()

sysconnect:{
 h:
 ip:
 u:
 $[(.cfg.proc.tipe=`broker)|
 (0<count exec i from .cfg.nodes where host=h, ipa=ip, u=.cfg.sysuser);
 [connupdate[];:1b]; 0b]
}

connupdate:{insert[`.cfg.sysconn;(h;ip;.z.w;.z.p;0Np)];}

.z.po:{sysconnect[];}
.z.pc:{update et:.z.p from `cfg.sysconn where host=h;h=.z.w;et=0Np;}
/
