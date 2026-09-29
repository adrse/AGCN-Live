(() => {
  const $ = (id) => document.getElementById(id);
  const $$ = (sel) => Array.from(document.querySelectorAll(sel));
  const fmt = new Intl.NumberFormat("pt-BR");

  const state = {
    monitoring:false, presenter:"stopped", startedAt:null, timer:null, autoTimer:null,
    viewers:0, likes:0, shares:0, comments:[], queue:[], currentSpeech:null,
    speechTick:null, speechEnd:null, salesIndex:0, interruptedTopic:null,
    reactiveStreak:0, productWindowUntil:0, lastSalesTopic:null,
    recentSalesSpeeches:[], purchaseSignals:0,
    product:{
      name:"Fone de Ouvido Bluetooth TWS", price:"129,90", regularPrice:"199,90",
      discount:"35% OFF", compatibility:"iPhone e Android",
      battery:"até 6 horas de uso por carga", warranty:"", shipping:"",
      stock:"7", liveOffer:true, liveOfferText:"Oferta exclusiva da LIVE",
      promotionNote:"",
      descriptionPoints:["Som de alta qualidade","Bateria de longa duração","Compatível com iPhone e Android"],
      benefits:"Áudio claro; sem fio; estojo compacto; fácil pareamento; confortável para uso diário",
      problems:"Ficar preso a fios; dificuldade para ouvir áudio com liberdade no dia a dia",
      included:"Fone TWS; estojo de carregamento; cabo de carregamento"
    }
  };

  const topics = [
    {key:"benefit",label:"Benefício → utilidade prática"},
    {key:"pain",label:"Dor → solução"},
    {key:"bundle",label:"Empilhar valor do kit"},
    {key:"battery",label:"Característica → benefício"},
    {key:"compatibility",label:"Quebrar objeção"},
    {key:"value",label:"Ancorar preço e desconto"},
    {key:"scarcity",label:"Escassez real"},
    {key:"social",label:"Prova social real"},
    {key:"cta",label:"Fechamento / CTA"}
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
    if(/comprei|finalizei|peguei um|peguei uma|garanti/.test(t)){intent="Compra confirmada";topic="purchase";priority=74;label="Média";}
    else if(/quero|comprar|manda.*link|onde compro|como compra/.test(t)){intent="Intenção de compra";topic="buy";priority=100;label="Alta";}
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
        return `${name}se já decidiu, confere o produto fixado e finaliza por ali.`;
      case "purchase":
        return `${name}parabéns pela compra! Boa escolha.`;
      case "benefit":
        return firstBenefit ? `${name}${firstBenefit}.` : null;
      default:
        return null;
    }
  }

  function splitItems(value){
    return String(value||"").split(/[;\n|]+/).map(x=>x.trim()).filter(Boolean);
  }

  function moneyNumber(value){
    const n=Number(String(value||"").replace(/\./g,"").replace(",",".").replace(/[^0-9.-]/g,""));
    return Number.isFinite(n) ? n : null;
  }

  function normalizedSpeech(text){
    return String(text||"").toLowerCase().normalize("NFD").replace(/[\u0300-\u036f]/g,"").replace(/[^a-z0-9 ]/g," ").replace(/\s+/g," ").trim();
  }

  function tooSimilarToRecent(text){
    const now=normalizedSpeech(text);
    const nowWords=now.split(" ").filter(Boolean);
    for(const prevRaw of state.recentSalesSpeeches.slice(-4)){
      const prev=normalizedSpeech(prevRaw);
      const prevWords=prev.split(" ").filter(Boolean);
      if(nowWords.length>=3 && prevWords.length>=3 && nowWords.slice(0,3).join(" ")===prevWords.slice(0,3).join(" ")) return true;
      const a=new Set(nowWords.filter(w=>w.length>=4));
      const b=new Set(prevWords.filter(w=>w.length>=4));
      const union=new Set([...a,...b]);
      const inter=[...a].filter(x=>b.has(x)).length;
      if(union.size && inter/union.size>=0.78) return true;
    }
    return false;
  }

  function hasRealScarcity(){
    const p=state.product;
    const stock=Number(p.stock);
    if(Number.isFinite(stock) && stock>=1 && stock<=10) return true;
    if(p.liveOffer) return true;
    return /últim|resta|relâmp|termina|encerra|só hoje|exclusiv|esgot|limitad/i.test(p.promotionNote||"");
  }

  function chooseSalesTopic(){
    const p=state.product;
    const stock=Number(p.stock);
    const available={
      benefit:splitItems(p.benefits).length>0 || (p.descriptionPoints||[]).length>0,
      pain:splitItems(p.problems).length>0,
      bundle:splitItems(p.included).length>0,
      battery:!!p.battery,
      compatibility:!!p.compatibility,
      value:!!p.price,
      scarcity:hasRealScarcity(),
      social:state.purchaseSignals>0,
      cta:true
    };

    const conversion=["scarcity","value","social","pain","bundle","cta"];
    if(state.salesIndex>0 && state.salesIndex%3===2){
      for(const key of conversion){
        if(available[key] && key!==state.lastSalesTopic) return key;
      }
    }

    for(let i=0;i<topics.length;i++){
      const key=topics[(state.salesIndex+i)%topics.length].key;
      if(available[key] && key!==state.lastSalesTopic) return key;
    }
    return "cta";
  }

  function proactiveText(topic,variant=0){
    const p=state.product;
    const points=p.descriptionPoints||[];
    const benefits=splitItems(p.benefits);
    const problems=splitItems(p.problems);
    const included=splitItems(p.included);
    const point=points.length ? points[(state.salesIndex+variant)%points.length] : "";
    const benefit=benefits.length ? benefits[(state.salesIndex+variant)%benefits.length] : point;
    const problem=problems.length ? problems[(state.salesIndex+variant)%problems.length] : "";
    const stock=Number(p.stock);

    if(topic==="scarcity"){
      if(Number.isFinite(stock) && stock>=1 && stock<=10){
        const openings=["Agora presta atenção nisso:","Só pra você ter noção,","E aqui tem um detalhe importante:"];
        return `${openings[variant%openings.length]} são ${stock} unidades disponíveis. Se você já decidiu, não deixa pra depois.`;
      }
      if(p.liveOffer && p.liveOfferText){
        return `${p.liveOfferText}. Se essa condição fez sentido pra você, aproveita enquanto ela está ativa na LIVE.`;
      }
      if(p.promotionNote) return `${p.promotionNote}. Então, se você já tava pensando em pegar, esse é o momento de conferir.`;
    }

    if(topic==="value" && p.price){
      if(p.regularPrice){
        return `Olha a diferença de valor: o preço normal é R$ ${p.regularPrice} e aqui tá R$ ${p.price}${p.discount ? ", com "+p.discount : ""}. É aí que essa oferta começa a ficar interessante.`;
      }
      return `Hoje ele tá por R$ ${p.price}${p.discount ? ", com "+p.discount : ""}. Pelo que entrega, é uma condição bem forte.`;
    }

    if(topic==="social" && state.purchaseSignals>0){
      return `Já tivemos ${state.purchaseSignals} confirmação${state.purchaseSignals===1?"":"ões"} de compra aqui no teste. Tem gente aproveitando enquanto a apresentação tá rolando.`;
    }

    if(topic==="pain" && problem){
      return `Se o que te incomoda é ${problem.toLowerCase()}, aí esse produto começa a fazer sentido: ${benefit || point || "ele foi pensado pra facilitar o uso no dia a dia"}.`;
    }

    if(topic==="bundle" && included.length){
      const sample=included.slice(0,3).join(", ");
      return `E não é só o ${p.name}: junto você leva ${sample}. Esse conjunto aumenta bastante o valor do que você tá levando.`;
    }

    if(topic==="battery" && p.battery){
      const starts=["Na prática, a bateria ajuda bastante:","Pra uso no dia a dia, olha isso:","Sobre autonomia, um ponto bom é:"];
      return `${starts[variant%starts.length]} ${p.battery}. Você não fica tão preso a carregamento toda hora.`;
    }

    if(topic==="compatibility" && p.compatibility){
      return `Pra não ter dúvida antes de comprar: ele funciona com ${p.compatibility}. Isso já elimina uma das principais dúvidas de compatibilidade.`;
    }

    if(topic==="benefit"){
      const starts=["O que chama atenção aqui é","Uma coisa que faz diferença no uso é","Pensando no dia a dia,"];
      if(benefit) return `${starts[variant%starts.length]} ${benefit.toLowerCase()}. É aquele tipo de detalhe que você percebe usando.`;
      if(point) return `${starts[variant%starts.length]} ${point.toLowerCase()}.`;
    }

    if(topic==="cta") return `Se o ${p.name} encaixou no que você precisa, confere o produto fixado e já finaliza enquanto a condição da LIVE estiver valendo.`;

    return `Vou te mostrar outro ponto do ${p.name}: ${point || benefit || "ele foi pensado pra facilitar o uso no dia a dia"}.`;
  }

  function enqueueComment(user,text){
    const item={id:Date.now()+Math.random(),user:user||"Visitante",text,at:Date.now(),decision:classify(text)};
    state.comments.unshift(item); state.comments=state.comments.slice(0,30);
    if(item.decision.topic==="purchase") state.purchaseSignals+=1;

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

    let topic=state.interruptedTopic || chooseSalesTopic();
    state.interruptedTopic=null;

    let text="";
    let chosen=topic;
    for(let attempt=0;attempt<5;attempt++){
      text=proactiveText(chosen,attempt);
      if(text && !tooSimilarToRecent(text)) break;
      state.salesIndex+=1;
      chosen=chooseSalesTopic();
    }

    if(!text) return;
    if(!inProductWindow()) state.reactiveStreak=0;

    state.lastSalesTopic=chosen;
    state.salesIndex+=1;
    state.recentSalesSpeeches.push(text);
    state.recentSalesSpeeches=state.recentSalesSpeeches.slice(-8);

    speak(text,{type:"proactive",topic:chosen});
    renderTopics();
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
    const currentIndex=Math.max(0,state.salesIndex%topics.length);
    const next=[];
    for(let i=0;i<topics.length && next.length<3;i++){
      const t=topics[(currentIndex+i)%topics.length];
      if(t.key!==state.lastSalesTopic) next.push(t);
    }
    $("next-topics").innerHTML=next.map((t,i)=>`<div>${i+1}. ${t.label}</div>`).join("");
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

  function pointValues(selector){
    return $$(selector).map(x=>x.value.trim()).filter(Boolean);
  }

  function addPointRow({
    containerId,
    inputClass,
    removeClass,
    placeholder,
    value=""
  }){
    const row=document.createElement("div");
    row.className="description-row";
    const input=document.createElement("input");
    input.className=inputClass;
    input.placeholder=placeholder;
    input.value=value;
    const remove=document.createElement("button");
    remove.type="button";
    remove.className=`secondary ${removeClass}`;
    remove.textContent="×";
    row.append(input,remove);
    $(containerId).appendChild(row);
  }

  function bindPointEditor({
    containerId,
    addButtonId,
    inputClass,
    removeClass,
    placeholder
  }){
    $(addButtonId).onclick=()=>addPointRow({
      containerId,
      inputClass,
      removeClass,
      placeholder
    });

    $(containerId).addEventListener("click",event=>{
      const button=event.target.closest("."+removeClass);
      if(!button) return;
      const container=$(containerId);
      const rows=Array.from(container.querySelectorAll(".description-row"));
      if(rows.length===1){
        const input=rows[0].querySelector("."+inputClass);
        if(input) input.value="";
        return;
      }
      button.closest(".description-row").remove();
    });
  }

  bindPointEditor({
    containerId:"description-points",
    addButtonId:"add-description",
    inputClass:"description-point",
    removeClass:"remove-description",
    placeholder:"Ex.: bateria de até 6 dias"
  });
  bindPointEditor({
    containerId:"benefit-points",
    addButtonId:"add-benefit",
    inputClass:"benefit-point",
    removeClass:"remove-benefit",
    placeholder:"Ex.: áudio claro mesmo em chamadas"
  });
  bindPointEditor({
    containerId:"problem-points",
    addButtonId:"add-problem",
    inputClass:"problem-point",
    removeClass:"remove-problem",
    placeholder:"Ex.: evita ficar preso a fios"
  });

  $("save-product").onclick=()=>{
    const points=pointValues(".description-point");
    const benefits=pointValues(".benefit-point");
    const problems=pointValues(".problem-point");
    state.product={
      name:$("product-name").value.trim(),
      price:$("product-price").value.trim(),
      regularPrice:$("product-regular-price").value.trim(),
      discount:$("product-discount").value.trim(),
      compatibility:$("product-compatibility").value.trim(),
      battery:$("product-battery").value.trim(),
      warranty:$("product-warranty").value.trim(),
      shipping:$("product-shipping").value.trim(),
      stock:$("product-stock").value.trim(),
      liveOffer:$("product-live-offer").checked,
      liveOfferText:$("product-live-offer-text").value.trim(),
      promotionNote:$("product-promotion-note").value.trim(),
      descriptionPoints:points,
      benefits:benefits.join("; "),
      problems:problems.join("; "),
      included:$("product-included").value.trim()
    };
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