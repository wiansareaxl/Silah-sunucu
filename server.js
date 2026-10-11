const http=require('http'),fs=require('fs'),path=require('path'),{WebSocketServer}=require('ws');
const PORT=process.env.PORT||3000,DB=path.join(process.env.DATA_DIR||__dirname,'stats.json');
let S={};try{S=JSON.parse(fs.readFileSync(DB,'utf8'))}catch(e){}
let dirty=0;setInterval(()=>{if(dirty){dirty=0;fs.writeFile(DB,JSON.stringify(S),()=>{})}},10000);
const st=c=>S[c.id]||(S[c.id]={n:c.n,k:0,d:0});
const top=(id,n)=>Object.entries(S).map(([i,s])=>({n:s.n,k:s.k,d:s.d,me:i===id})).sort((a,b)=>b.k-a.k||a.d-b.d).slice(0,n);
const srv=http.createServer((q,r)=>fs.readFile(path.join(__dirname,'Silah_Oyunu_3D.html'),(e,d)=>{
  if(e){r.writeHead(200);return r.end('Silah sunucusu calisiyor')}r.writeHead(200,{'content-type':'text/html; charset=utf-8'});r.end(d)}));
const wss=new WebSocketServer({server:srv,maxPayload:4096});
const clients=new Set(),rooms=new Map(),rdirty=new Set();let ld=0,nid=1;
function leave(c){if(!c.room)return;const r=rooms.get(c.room);if(r){r.delete(c);if(r.size)rdirty.add(c.room);else rooms.delete(c.room)}c.room=null;c.pp=null}
wss.on('connection',ws=>{
  const c={ws,peer:'p'+nid++,id:'p'+nid,n:'?',lp:null,room:null,pp:null,ld:0};clients.add(c);
  const send=o=>{if(ws.readyState===1)ws.send(JSON.stringify(o))};c.send=send;send({t:'me',peer:c.peer});
  ws.on('message',raw=>{let m;try{m=JSON.parse(raw)}catch(e){return}if(!m||typeof m!=='object')return;
    if(m.t==='hello'){c.id=String(m.id||c.id).slice(0,40);c.n=String(m.n||'?').slice(0,14)}
    else if(m.t==='lobbyP'){c.lp=m.p&&typeof m.p==='object'?m.p:null;if(c.lp&&c.lp.n)c.n=String(c.lp.n).slice(0,14);ld=1}
    else if(m.t==='join'){leave(c);const k=String(m.room||'m-arena').slice(0,24);let r=rooms.get(k);if(!r)rooms.set(k,r=new Set());r.add(c);c.room=k}
    else if(m.t==='leave')leave(c);
    else if(m.t==='pres'&&c.room&&m.p&&typeof m.p==='object'){c.pp=m.p;if(m.p.n)c.n=String(m.p.n).slice(0,14);rdirty.add(c.room)}
    else if(m.t==='lb')send({t:'lb',rows:top(c.id,30)});
    else if(m.t==='ev'&&c.room){const r=rooms.get(c.room),d=m.d||{};if(!r)return;
      if(m.k==='hit'){for(const o of r)if(o!==c&&o.peer===d.to)o.send({t:'ev',k:'hit',from:c.peer,data:{to:d.to,d:Math.min(100,+d.d||0)}})}
      else if(m.k==='dead'){const t=Date.now();if(t-c.ld<2000)return;c.ld=t;const v=st(c);v.d++;v.n=c.n;
        for(const o of r)if(o!==c&&o.peer===d.by){const s=st(o);s.k++;s.n=o.n}dirty=1;
        const data={by:d.by,vn:String(d.vn||'').slice(0,14),kn:String(d.kn||'').slice(0,14),x:+d.x||0,z:+d.z||0,a:+d.a||0,ct:!!d.ct};
        for(const o of r)o.send({t:'ev',k:'dead',from:c.peer,data})}}});
  ws.on('close',()=>{leave(c);clients.delete(c);ld=1});ws.on('error',()=>{});
});
setInterval(()=>{for(const k of rdirty){const r=rooms.get(k);if(!r)continue;const p=[];for(const o of r)if(o.pp)p.push([o.peer,o.pp]);
  const s=JSON.stringify({t:'room',p});for(const o of r)if(o.ws.readyState===1)o.ws.send(s)}rdirty.clear()},66);
setInterval(()=>{if(!ld)return;ld=0;const p=[];for(const o of clients)if(o.lp)p.push([o.peer,o.lp]);
  const s=JSON.stringify({t:'lobby',p});for(const o of clients)if(o.lp&&o.ws.readyState===1)o.ws.send(s)},500);
setInterval(()=>wss.clients.forEach(w=>{try{w.ping()}catch(e){}}),25000);
srv.listen(PORT,()=>console.log('Sunucu hazir: port '+PORT));
