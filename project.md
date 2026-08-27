# Motor de detecção de transações suspeitas

## Exigencias techs
- 8000 TPS(pico 25k tps) com alertas <= 500ms
- Segurança de ponta a ponta
    - crypto em transito/repouso
    - auth entre serviços
    - LGPD
- Indepotencia
- Extencibilidade da regra sem redeploy
- Monitoramento SRE e respostas a incidentes

## O que é esperado
- Capacidade de estruturar sistemas back distribuidos e orientados a eventos
- Qualidade nas decisões de arquiteturas e trade-offs
- Tratamento de falhas, resiliência e estratégia de recuperação
- Clareza na modelagem de dados e organização do código
- Estratégia de testes e cobertura de cenários críticos
- Equilibrio entre simplicidade e robustez

## Desafio proposto
- Receber eventos de transações em tempo real
- Aplicar lógica de detecção de transações suspeitas
- Gerar e enviar alertas e canais internos e externos
- Seguir operação Se um serviço auxiliar cair
- Não é obrigatório uso de banco de dados

## Contexto de negócios
- É necessário que o módulo identifique transações suspeitas em tempo real, processando eventos de outros serviços internos e gerando alertas automaticos para equipe antifraude e para o cliente.

## Ferramentoas e recursos, não é obrigatório o uso
- AWS, ecossistema inteiro
- Serviço para busca dos dados de cliente(CC) por id
- Gateway com lambda authorizer para validação de token
- Ferramenta com kafka coorporativo(kafka as a service)

## O que eu(candidato) planejo para apresentação
- Documento com a proposta
- Desenho de arquitetura
- App principal com lógica de detecção com conexões mockadas