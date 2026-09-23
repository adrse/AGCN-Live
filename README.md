# AGCN LIVE

Painel operacional web de uma única página. Os arquivos em `backend/original/` são cópias integrais das células de código 02–18 do notebook original; `manifest.json` registra os hashes. `backend/runtime.py` executa as células 02–17 e as funções operacionais da Interface V8 em ordem, cada visitante em um namespace Python isolado. Somente as chamadas de registro/renderização do Google Colab são substituídas. A interface está em `dist/`.

## Fluxos preservados

- Shopee Worker → Live Engine → Workers/Coaches → Context Fusion → Decision Coach → LIVE COACH.
- Link de produto Shopee ou TikTok Shop → navegador de produto da plataforma → Product Extractor → Product Sales Builder → Sales Decision Coach → SALES COACH.
- TikTok Worker → Live Engine → LIVE COACH. O fluxo de vendas permanece separado do Context Fusion e só começa após o usuário informar um link de produto da mesma plataforma.

O endpoint de estado consulta a Interface V8 e o histórico do Live Engine. Apenas a bridge original da V8 consome a fila final de Sales; a API e a SSE não consomem filas internas. Preço, informação adicional e fatos estruturados enviados pelo vendedor são encaminhados exclusivamente ao Product Extractor. Conflitos de preço preservam as duas origens. O frontend só exibe orientações recebidas do Decision Coach e do Sales Decision Coach e respeita `timestamp`, `display_seconds` e `expires_at`.

`backend/product_link.py` mantém páginas de Shopee e TikTok Shop no Chromium do servidor por sessão, abre o link informado na aba correspondente e valida os dados visíveis antes de entregar um evento ao Product Extractor. Links curtos `vt.tiktok.com` e `s.shopee.com.br` são resolvidos com redirecionamentos limitados a HTTPS e aos domínios oficiais da mesma plataforma; o destino final precisa ter um identificador de produto válido. Parâmetros de rastreamento são removidos. O Worker de produto antigo continua preservado em `backend/original/14_shopee_product.py`, mas sua captura automática da LIVE não é iniciada nesta modalidade por link. Os produtores não têm outro consumidor das filas originais. O site não utiliza credenciais de TikTok: somente páginas de produto acessíveis sem login podem ser lidas. Desafios de tráfego, logins e campos ausentes são expostos como estado de erro ou vazio; não se adivinha preço nem ficha técnica.

## Rodar / publicar o serviço completo

```bash
docker build -t agcn-live .
docker run --rm -p 8000:8000 -e PORT=8000 agcn-live
```

Abra `http://localhost:8000`. Para produção, implante este diretório como serviço **Docker** em um host que execute processos Python, possua Chromium e permita conexão de saída às plataformas de LIVE. Exponha a porta indicada pela variável `PORT` via HTTPS. O mesmo serviço serve o frontend e a API, assim um domínio personalizado pode apontar para o serviço sem CORS ou cookies entre sites. Não há login. Identificadores aleatórios em `sessionStorage` separam monitoramentos de visitantes; por padrão até oito sessões simultâneas por processo. Sessões sem atividade por uma hora são encerradas. As sessões, históricos e processos são efêmeros e exigem implantação com uma instância, sem autoscaling horizontal. Ajuste `AGCN_MAX_SESSIONS` de acordo com RAM/CPU e regras do provedor. Não use a versão `dist/` isoladamente como implantação funcional: ela necessita da API Python `/api/*`.

Para teste sem Docker, instale os pacotes de `requirements.txt`, execute `python -m playwright install chromium` e rode `python -m backend.server`. A imagem Docker já inclui Chromium e suas bibliotecas; sua versão coincide com a versão fixada do Playwright. O servidor mostra erro explícito ao tentar iniciar um capturador cuja dependência não esteja instalada.

## Adaptações pontuais

`backend/runtime.py` neutraliza importações e chamadas próprias do Colab, sem alterar os arquivos originais. Faz três correções isoladas: normalizadores de estilo/texto usavam chaves de dois caracteres em `str.maketrans` e lançavam `ValueError`; globais `_stop_requested` e `_output_seq` colidiam entre Builder e Sales Decision na namespace compartilhada; links completos `live.shopee.com.br` com `session` válido passam ao resolvedor sem o encurtador. Nenhuma lógica de geração de sugestões, classificação ou coleta de eventos foi reescrita.

O fluxo por link depende do acesso do Chromium às páginas públicas; a Shopee pode retornar uma verificação de tráfego, caso em que o painel informa o bloqueio e não gera orientação de venda sem fatos verificados. A extração do TikTok Shop também pode variar conforme a região e a página disponibilizada ao servidor.
