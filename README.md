Скачать дипсик

мак
brew install ollama
ollama serve   # запустить фоновый сервис

линукс
curl -fsSL https://ollama.com/install.sh | sh
ollama serve


ollama pull deepseek-r1:8b
ollama run deepseek-r1:8b


проверка работоспособности
curl -s http://localhost:11434/api/tags | jq

пробный запрос к /api/generate
curl -s http://localhost:11434/api/generate \
  -d '{"model":"deepseek-r1:8b","prompt":"Скажи ok и больше ничего.","stream":false}'
