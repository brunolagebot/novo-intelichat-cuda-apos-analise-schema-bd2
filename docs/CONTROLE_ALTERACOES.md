# 🧱 ROTEIRO PADRÃO: CONTROLE DE ALTERAÇÕES CRÍTICAS EM APLICAÇÕES

## 1. ANTES DE QUALQUER MUDANÇA
 Crie uma nova branch baseada na main ou na branch de produção:

```bash
git checkout main
git pull
git checkout -b nome-da-feature-critica
```
 Documente a intenção da alteração em um arquivo `CHANGELOG.md` ou `registro_de_alteracoes.txt`, com:

*   Data
*   Responsável
*   Objetivo da alteração
*   Possíveis riscos

## 2. FASE DE DESENVOLVIMENTO
 Faça alterações em commits pequenos e bem descritos:

```bash
git commit -m "Corrige lógica de cálculo do frete no módulo X"
```
 Use `git status` e `git diff` com frequência para revisar o que foi alterado.

 Teste localmente (mínimo: testes manuais funcionais).

## 3. SALVANDO UM PONTO DE RETORNO
 Marque o último estado estável com uma tag:

```bash
git tag -a pre-alteracao-X -m "Último ponto estável antes de mudanças no módulo X"
git push origin pre-alteracao-X
```

## 4. DEPLOY EM AMBIENTE DE TESTE (HOMOLOGAÇÃO)
 Suba a branch para o GitHub:

```bash
git push origin nome-da-feature-critica
```
 Faça o deploy em ambiente de teste.

 Registre evidência de funcionamento (prints, logs, vídeos curtos, testes de aceitação).

## 5. APROVAÇÃO
 Peça revisão de código (pull request) e aprovação de outro desenvolvedor.

 Atualize o `CHANGELOG.md` com:

*   Alterações feitas
*   Testes executados
*   Confirmação da aprovação

## 6. MERGE COM MAIN OU PRODUÇÃO
 Faça o merge:

```bash
git checkout main
git pull
git merge nome-da-feature-critica
git push origin main
```
 Opcional (mas recomendado): crie nova tag de versão:

```bash
git tag -a v1.3.0 -m "Versão com alterações no módulo X"
git push origin v1.3.0
```

## 7. SE DER PROBLEMA
 Reverter com `git revert` (para manter histórico):

```bash
git revert <hash-do-commit>
```
 Ou voltar para o ponto marcado na tag:

```bash
git checkout pre-alteracao-X
``` 