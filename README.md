# AGCN LIVE

Painel operacional web de uma única página. Os arquivos em `backend/original/` são cópias integrais das células de código 02–18 do notebook original; `manifest.json` registra os hashes. `backend/runtime.py` executa as células 02–17 e as funções operacionais da Interface V8 em ordem, cada visitante em um namespace Python isolado. Somente as chamadas de registro/renderização do Google Colab são substituídas. A interface está em `dist/`.

## Fluxos preservados

- Shopee Worker → Live Engine → Workers/Coaches → Context Fusion → Decision Coach → LIVE COACH.
- Worker Shopee Produto → Product Extractor → Product Sales Builder → Sales Decision Coach → SALES COACH.
- TikTok Worker → Live Engine → LIVE COACH. Nenhum processo do Sales Coach é iniciado no TikTok.

O endpoint de estado consulta a Interface V8 e o histórico do Live Engine. Apenas a bridge original da V8 consome a fila final de Sales; a API e a SSE não consomem filas internas. Preço, informação adicional e fatos estruturados enviados pelo vendedor são encaminhados exclusivamente ao Product Extractor. Conflitos de preço preservam as duas origens. O frontend só exibe orientações recebidas do Decision Coach e do Sales Decision Coach e respeita `timestamp`, `display_seconds` e `expires_at`.

## Rodar / publicar o serviço completo

```bash
docker build -t agcn-live .
docker run --rm -p 8000:8000 -e PORT=8000 agcn-live
```

Abra `http://localhost:8000`. Para produção, implante este diretório como serviço **Docker** em um host que execute processos Python, possua Chromium e permita conexão de saída às plataformas de LIVE. Exponha a porta indicada pela variável `PORT` via HTTPS. O mesmo serviço serve o frontend e a API, assim um domínio personalizado pode apontar para o serviço sem CORS ou cookies entre sites. Não há login. Identificadores aleatórios em `sessionStorage` separam monitoramentos de visitantes; por padrão até oito sessões simultâneas por processo. Sessões sem atividade por uma hora são encerradas. As sessões, históricos e processos são efêmeros e exigem implantação com uma instância, sem autoscaling horizontal. Ajuste `AGCN_MAX_SESSIONS` de acordo com RAM/CPU e regras do provedor. Não use a versão `dist/` isoladamente como implantação funcional: ela necessita da API Python `/api/*`.

Para teste sem Docker, instale os pacotes de `requirements.txt`, execute `python -m playwright install chromium` e rode `python -m backend.server`. A imagem Docker já inclui Chromium e suas bibliotecas; sua versão coincide com a versão fixada do Playwright. O servidor mostra erro explícito ao tentar iniciar um capturador cuja dependência não esteja instalada.

## Adaptações pontuais

`backend/runtime.py` neutraliza importações e chamadas próprias do Colab, sem alterar os arquivos originais. Faz três correções isoladas: normalizadores de estilo/texto usavam chaves de dois caracteres em `str.maketrans` e lançavam `ValueError`; globais `_stop_requested` e `_output_seq` colidiam entre Builder e Sales Decision na namespace compartilhada; links completos `live.shopee.com.br` com `session` válido passam ao resolvedor sem o encurtador. Nenhuma lógica de geração de sugestões, classificação ou coleta de eventos foi reescrita.

O Worker Shopee Produto não foi confirmado em LIVE real neste ambiente; quando não identifica um produto ou encontra erro HTTP, o painel informa o estado e não fabrica produto, preço ou orientação de vendas.
