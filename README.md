# Multicast Confiável com Relógio de Lamport

Este projeto implementa um sistema de multicast confiável usando o algoritmo de Relógio de Lamport para ordenação de mensagens em um sistema distribuído.

## Requisitos

- Python
- Git

## Baixar o reposit'rio

- Clone o repositório:

```bash
git clone https://github.com/raquelpfm/lamport
cd lamport
```

## Como Executar

### Exemplo com 3 Processos

- Abra três terminais diferentes
-  Em cada terminal, navegue até a pasta do projeto:

   ```bash
   cd lamport
   ```

- Execute os comandos em cada terminal:

   Terminal 1:

   ```bash
   python3 process.py P1 8001 P2:8002 P3:8003
   ```

   Terminal 2:

   ```bash
   python3 process.py P2 8002 P1:8001 P3:8003
   ```

   Terminal 3:

   ```bash
   python3 process.py P3 8003 P1:8001 P2:8002
   ``

   ```

- Aguarde alguns segundos para as conexões serem estabelecidas entre os processos
-  Para enviar uma mensagem, digite em qualquer terminal:
   ```
   multicast [ mensagem ]
   ```