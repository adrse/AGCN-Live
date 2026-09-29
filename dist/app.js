(() => {
  const $ = (id) => document.getElementById(id);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));
  const fmt = new Intl.NumberFormat("pt-BR");

  const state = {
    monitoring:false, presenter:"stopped", startedAt:null, timer:null, autoTimer:null,
    viewers:0, likes:0, shares:0, comments:[], queue:[], currentSpeech:null,
    speechTick:null, speechEnd:null, salesIndex:0, interruptedTopic:null,
    reactiveStreak:0, productWindowUntil:0,
    product:{
      name:"Fone de Ouvido Bluetooth TWS", price:"129,90", regularPrice:"199,90",
      discount:"35% OFF", compatibility:"iPhone e Android",
      battery:"até 6 horas de uso por carga", warranty:"", shipping:"",
      descriptionPoints:["Som de alta qualidade","Bateria de longa duração","Compatível com iPhone e Android"],
      benefits:"Áudio claro; sem fio; estojo compacto; fácil pareamento; confortável para uso diário"
    }
  };

  const topics = [
    {key:"benefit",label:"Destacar benefícios do produto"},
    {key:"battery",label:"Falar de bateria e uso"},
    {key:"compatibility",label:"Mostrar compatibilidade"},
    {key:"value",label:"Reforçar oferta e preço"},
    {key:"cta",label:"Chamar para ação"}
  ];

  const quickUsers = ["Maria","Lucas","Ana","Carlos","Beatriz","Rafael","Nadjane","Gabriel"];
  const quickTexts = ["qual o valor?","serve pra iphone?","manda o link","quanto dura a bateria?","tem garantia?","quero comprar","tem frete grátis?","é bom mesmo?"];

  function toast(msg){ const t=$("toast"); t.textContent=msg; t.classList.add("show"); setTimeout(()=>t.classList.remove("show"),1800); }
  function screen(name){ $$(".screen").forEach(x=>x.classList.remove("active")); $$(".nav-btn").forEach(x=>x.classList.toggle("active",x.dataset.screen===name)); $("screen-"+name).classList.add("active"); }
  $$(".nav-btn").forEach(b=>b.onclick=()=>screen(b.dataset.screen));
  $$("[data-go]").forEach(b=>b.onclick=()=>screen(b.dataset.go));

  function productFact(key){
    const p=state.product;
    return {price:p.price,compatibility:p.compatibility,battery:p.battery,warranty:p.warranty,shipping:p.shipping}[key] || "";
  }

  function classify(text){
    const t=text.toLowerCase();
    let intent="comentário",topic="general",priority=35,label="Baixa";
    if(/quero|comprar|manda.*link|onde compro|como compra/.test(t)){intent="Intenção de compra";topic="buy";priority=100;label="Alta";}
    else if(/preço|preco|valor|quanto custa/.test(t)){intent="Pergunta de preço";topic="price";priority=95;label="Alta";}
    else if(/serve|compat|iphone|android/.test(t)){intent="Compatibilidade";topic="compatibility";priority=82;label="Média";}
    else if(/bateria|dura|horas/.test(t)){intent="Duração da bateria";topic="battery";priority=78;label="Média";}
    else if(/garantia/.test(t)){intent="Garantia";topic="warranty";priority=80;label="Média";}
    else if(/frete|entrega/.test(t)){intent="Frete/entrega";topic="shipping";priority=84;label="Média";}
    else if(/bom|vale|qualidade/.test(t)){intent="Objeção/qualidade";topic="benefit";priority=68;label="Média";}
    return {intent,topic,priority,label};
  }

  function responseFor(comment,decision){
    const name=$("call-name").checked ? comment.user+", " : "";
    const p=state.product;
    const firstBenefit=(p.benefits||"").split(/[;\n|]+/).map(x=>x.trim()).find(Boolean);
    switch(decision.topic){
      case "price":
        return p.price ? `${name}hoje ele tá por R$ ${p.price}${p.discount ? ", com "+p.discount : ""}.` : null;
      case "compatibility":
        return p.compatibility ? `${name}serve sim. ${p.compatibility}.` : null;
      case "battery":
        return p.battery ? `${name}a bateria dele dura ${p.battery}.` : null;
      case "warranty":
        return p.warranty ? `${name}tem sim, garantia de ${p.warranty}.` : null;
      case "shipping":
        return p.shipping ? `${name}${p.shipping}.` : null;
      case "buy":
        return `${name}boa! Se quiser pegar, confere o produto fixado na LIVE.`;
      case "benefit":
        return firstBenefit ? `${name}${firstBenefit}.` : null;
      default:
        return null;
    }
  }

  function proactiveText(topic){
    const p=state.product;
    const points=p.descriptionPoints||[];
    const point=points.length ? points[state.salesIndex % points.length] : "";
    const firstBenefit=(p.benefits||"").split(/[;\n|]+/).map(x=>x.trim()).find(Boolean) || "";
    if(topic==="battery" && p.battery) return `E olha a bateria dele: ${p.battery}. Dá pra usar bem tranquilo no dia a dia.`;
    if(topic==="compatibility" && p.compatibility) return `Outra coisa boa: ele funciona com ${p.compatibility}. Então é bem prático pra usar no dia a dia.`;
    if(topic==="value" && p.price) return `Hoje ele tá por R$ ${p.price}${p.discount ? ", com "+p.discount : ""}. Vale olhar com carinho essa oferta.`;
    if(topic==="cta") return `Se curtiu o ${p.name}, dá uma olhada no produto fixado aí na LIVE.`;
    if(point) return `Olha esse detalhe do ${p.name}: ${point}.`;
    if(firstBenefit) return `Uma coisa legal nele é ${firstBenefit}.`;
    return `Olha só o ${p.name}. Vou te mostrando os principais pontos dele por aqui.`;
  }

  function enqueueComment(user,text){
    const item={id:Date.now()+Math.random(),user:user||"Visitante",text,at:Date.now(),decision:classify(text)};
    state.comments.unshift(item); state.comments=state.comments.slice(0,30);

    // Pergunta sem resposta conhecida aparece no chat, mas não vira fala.
    const answer=responseFor(item,item.decision);
    item.ignored=!answer;
    if(answer){
      state.queue.push(item);
      state.queue.sort((a,b)=>b.decision.priority-a.decision.priority);
      if(state.queue.length>40) state.queue=state.queue.slice(0,40);
    }

    renderComments(); renderQueue(); updateCPM(); renderCadence();
    if(state.presenter==="running" && $("auto-replies").checked) processQueue();
  }

  function inProductWindow(){
    if(state.productWindowUntil && Date.now()>=state.productWindowUntil){
      state.productWindowUntil=0;
      state.reactiveStreak=0;
    }
    return state.productWindowUntil>Date.now();
  }

  function startProductWindow(){
    state.productWindowUntil=Date.now()+30000;
    renderCadence();
  }

  function processQueue(){
    if(state.currentSpeech || state.presenter!=="running") return;

    if(inProductWindow()){
      proactive();
      return;
    }

    const next=state.queue.shift();
    if(next && ($("prioritize-questions").checked || next.decision.priority>=80)){
      const answer=responseFor(next,next.decision);
      renderQueue();
      if(answer){
        state.interruptedTopic=topics[state.salesIndex % topics.length].key;
        state.reactiveStreak+=1;
        speak(answer,{type:"comment",topic:next.decision.topic,returnTo:state.interruptedTopic});
        renderCadence();
        return;
      }
    }

    proactive();
  }

  function proactive(){
    if(state.currentSpeech || state.presenter!=="running") return;
    let topic;
    if(state.interruptedTopic){ topic=state.interruptedTopic; state.interruptedTopic=null; }
    else { topic=topics[state.salesIndex % topics.length].key; state.salesIndex++; }
    if(!inProductWindow()) state.reactiveStreak=0;
    speak(proactiveText(topic),{type:"proactive",topic});
    renderCadence();
  }

  function renderCadence(){
    const mode=$("cadence-mode");
    const countdown=$("cadence-countdown");
    if(!mode || !countdown) return;
    if(inProductWindow()){
      const sec=Math.max(0,Math.ceil((state.productWindowUntil-Date.now())/1000));
      mode.textContent="Modo produto";
      countdown.style.display="block";
      countdown.textContent=`${sec}s sem responder comentários`;
    }else{
      mode.textContent=`Modo interativo · ${state.reactiveStreak}/3 respostas`;
      countdown.style.display="none";
      countdown.textContent="";
    }
  }

  function speak(text,meta={}){
    if(state.presenter!=="running") return;
    const speed=Number($("voice-speed").value)/100;
    const seconds=Math.max(2.4,Math.min(14,text.length/(15*speed)));
    state.currentSpeech={text,meta,start:performance.now(),duration:seconds*1000};
    $("speaking-text").textContent=text; $("on-air-badge").textContent="Ao vivo"; $("on-air-badge").className="badge green";
    if($("browser-audio").checked && "speechSynthesis" in window){
      speechSynthesis.cancel();
      const u=new SpeechSynthesisUtterance(text); u.lang="pt-BR"; u.rate=Math.min(1.8,Math.max(.8,speed)); speechSynthesis.speak(u);
    }
    clearInterval(state.speechTick); clearTimeout(state.speechEnd);
    state.speechTick=setInterval(()=>{
      if(!state.currentSpeech) return;
      const elapsed=performance.now()-state.currentSpeech.start;
      const pct=Math.min(100,elapsed/state.currentSpeech.duration*100);
      $("speech-progress").style.width=pct+"%";
      $("speech-timer").textContent=`${fmtClock(elapsed/1000)} / ${fmtClock(state.currentSpeech.duration/1000)}`;
    },120);
    state.speechEnd=setTimeout(()=>{
      const finishedMeta=state.currentSpeech ? state.currentSpeech.meta : {};
      clearInterval(state.speechTick); state.currentSpeech=null; $("speech-progress").style.width="0%"; $("on-air-badge").textContent="silêncio"; $("on-air-badge").className="badge neutral";
      if(finishedMeta.type==="comment" && state.reactiveStreak>=3 && !inProductWindow()){
        startProductWindow();
      }
      setTimeout(processQueue,450);
    },seconds*1000);
  }

  function renderComments(){
    const root=$("comments-list");
    if(!state.comments.length){root.className="comments-list empty-state";root.textContent="Os comentários aparecerão aqui.";return;}
    root.className="comments-list"; root.innerHTML=state.comments.map(c=>`<div class="comment-item"><b>${escapeHTML(c.user)}</b><p>${escapeHTML(c.text)}</p><small class="muted">${c.ignored ? "ignorado — sem resposta conhecida" : c.decision.intent}</small></div>`).join("");
  }
  function renderQueue(){
    const root=$("queue-list");
    if(!state.queue.length){root.className="queue-list empty-state";root.textContent="Nenhum comentário priorizado.";return;}
    root.className="queue-list"; root.innerHTML=state.queue.slice(0,8).map((c,i)=>`<div class="queue-item"><div><b>${i+1}. ${escapeHTML(c.decision.intent)}</b><p>“${escapeHTML(c.text)}”</p></div><span class="badge ${c.decision.label==="Alta"?"high":c.decision.label==="Média"?"medium":"low"}">${c.decision.label}</span></div>`).join("");
  }
  function renderTopics(){
    $("next-topics").innerHTML=topics.slice(0,3).map((t,i)=>`<div>${i+1}. ${t.label}</div>`).join("");
  }
  function updateCPM(){
    const cutoff=Date.now()-60000; const n=state.comments.filter(c=>c.at>=cutoff).length;
    $("metric-cpm").textContent=n; $("comments-rate").textContent=n+"/min";
  }
  function fmtClock(s){s=Math.max(0,Math.floor(s));return String(Math.floor(s/60)).padStart(2,"0")+":"+String(s%60).padStart(2,"0")}
  function fmtLive(s){s=Math.max(0,Math.floor(s));return [Math.floor(s/3600),Math.floor(s%3600/60),s%60].map(v=>String(v).padStart(2,"0")).join(":")}
  function escapeHTML(s){return String(s??"").replace(/[&<>"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[m]));}

  function updateMetrics(){
    if(!state.monitoring) return;
    state.viewers += Math.floor(Math.random()*8); state.likes += Math.floor(Math.random()*40); if(Math.random()<.25) state.shares++;
    $("metric-viewers").textContent=fmt.format(state.viewers); $("metric-likes").textContent=fmt.format(state.likes); $("metric-shares").textContent=fmt.format(state.shares);
    if(state.startedAt) $("metric-time").textContent=fmtLive((Date.now()-state.startedAt)/1000);
    updateCPM(); renderCadence();
  }

  $("monitor-btn").onclick=()=>{state.monitoring=true;if(!state.startedAt)state.startedAt=Date.now();$("connection-label").textContent="Monitorando LIVE simulada";toast("Monitoramento iniciado");};
  $("stop-monitor-btn").onclick=()=>{state.monitoring=false;$("connection-label").textContent="Pronto para simular";toast("Monitoramento parado");};
  $("presenter-start").onclick=()=>{state.presenter="running";$("presenter-state").textContent="Apresentando";$("presenter-state").className="badge green";processQueue();};
  $("presenter-pause").onclick=()=>{state.presenter=state.presenter==="paused"?"running":"paused";if(state.presenter==="paused"){clearTimeout(state.speechEnd);clearInterval(state.speechTick);state.currentSpeech=null;if("speechSynthesis" in window)speechSynthesis.cancel();$("presenter-state").textContent="Pausado";$("presenter-state").className="badge medium";$("on-air-badge").textContent="pausado";}else{$("presenter-state").textContent="Apresentando";$("presenter-state").className="badge green";processQueue();}};
  $("presenter-takeover").onclick=()=>{state.presenter="takeover";clearTimeout(state.speechEnd);clearInterval(state.speechTick);state.currentSpeech=null;if("speechSynthesis" in window)speechSynthesis.cancel();$("presenter-state").textContent="Você assumiu";$("presenter-state").className="badge blue";$("speaking-text").textContent="IA em silêncio. O monitoramento continua e você pode devolver a apresentação clicando em Iniciar.";};

  $("send-comment").onclick=()=>{const text=$("comment-text").value.trim();if(!text)return;enqueueComment($("comment-user").value.trim()||"Visitante",text);$("comment-text").value="";};
  $("comment-text").addEventListener("keydown",e=>{if(e.key==="Enter")$("send-comment").click();});
  $$("[data-comment]").forEach(b=>b.onclick=()=>enqueueComment($("comment-user").value.trim()||"Visitante",b.dataset.comment));

  function descriptionPointValues(){
    return $(".description-point").map(x=>x.value.trim()).filter(Boolean);
  }

  function addDescriptionPoint(value=""){
    const row=document.createElement("div");
    row.className="description-row";
    const input=document.createElement("input");
    input.className="description-point";
    input.placeholder="Ex.: bateria de até 6 dias";
    input.value=value;
    const remove=document.createElement("button");
    remove.type="button";
    remove.className="secondary remove-description";
    remove.textContent="×";
    row.append(input,remove);
    $("description-points").appendChild(row);
  }

  $("add-description").onclick=()=>addDescriptionPoint();
  $("description-points").addEventListener("click",event=>{
    const button=event.target.closest(".remove-description");
    if(!button) return;
    const rows=$(".description-row");
    if(rows.length===1){
      rows[0].querySelector(".description-point").value="";
      return;
    }
    button.closest(".description-row").remove();
  });

  $("save-product").onclick=()=>{
    const points=descriptionPointValues();
    state.product={name:$("product-name").value.trim(),price:$("product-price").value.trim(),regularPrice:$("product-regular-price").value.trim(),discount:$("product-discount").value.trim(),compatibility:$("product-compatibility").value.trim(),battery:$("product-battery").value.trim(),warranty:$("product-warranty").value.trim(),shipping:$("product-shipping").value.trim(),descriptionPoints:points,benefits:$("product-benefits").value.trim()};
    $("active-product-name").textContent=state.product.name;$("active-product-desc").textContent=points.slice(0,2).join(" • ")||"Sem descrição";$("active-product-price").textContent=state.product.price?"R$ "+state.product.price:"—";$("active-product-discount").textContent=state.product.discount||"";$("product-save-status").textContent="Produto salvo e ativado.";toast("Produto ativo atualizado");
  };

  function refreshVoice(){
    const female=$("voice-profile").value==="female"; const speed=(Number($("voice-speed").value)/100).toFixed(2).replace(".",",");
    $("speed-value").textContent=speed+"x"; $("voice-summary").textContent=(female?"Vivian — Feminina HQ":"Ryan — Masculina HQ"); $("voice-meta").textContent=`Qwen3-TTS 1.7B · ${speed}x · HQ no Windows`;
  }
  $("voice-profile").onchange=refreshVoice;$("voice-speed").oninput=refreshVoice;

  function manageAutoComments(){
    clearInterval(state.autoTimer);state.autoTimer=null;
    if(!$("auto-comments").checked)return;
    const sec=Number($("auto-comment-interval").value)||7;
    state.autoTimer=setInterval(()=>{if(!state.monitoring)return;enqueueComment(quickUsers[Math.floor(Math.random()*quickUsers.length)],quickTexts[Math.floor(Math.random()*quickTexts.length)]);},sec*1000);
  }
  $("auto-comments").onchange=manageAutoComments;$("auto-comment-interval").onchange=manageAutoComments;
  $("reset-lab").onclick=()=>location.reload();

  setInterval(updateMetrics,1000); renderComments();renderQueue();renderTopics();refreshVoice();renderCadence();
})();