import json
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from tqdm import tqdm

class TrainingDataAnalyzer:
    def __init__(self, data_path: str):
        """
        Inicializa o analisador de dados de treinamento.
        
        Args:
            data_path: Caminho para o arquivo JSON com os dados
        """
        self.data_path = data_path
        self.data = self._load_data()
        self.output_dir = self._create_output_dir()
        
    def _load_data(self) -> List[Dict[str, Any]]:
        """Carrega os dados do arquivo JSON."""
        with open(self.data_path, 'r', encoding='utf-8') as f:
            return [json.loads(line) for line in f]
            
    def _create_output_dir(self) -> Path:
        """Cria diretório de saída com timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(f"output_{timestamp}")
        output_dir.mkdir(exist_ok=True)
        return output_dir
        
    def analyze_message_lengths(self):
        """Analisa distribuição do comprimento das mensagens."""
        user_lengths = []
        assistant_lengths = []
        
        for item in tqdm(self.data, desc="Analisando comprimentos"):
            for msg in item['messages']:
                length = len(msg['content'])
                if msg['role'] == 'user':
                    user_lengths.append(length)
                elif msg['role'] == 'assistant':
                    assistant_lengths.append(length)
                    
        plt.figure(figsize=(12, 6))
        plt.subplot(1, 2, 1)
        sns.histplot(user_lengths, bins=50)
        plt.title('Distribuição de Comprimento - Mensagens do Usuário')
        plt.xlabel('Comprimento (caracteres)')
        plt.ylabel('Contagem')
        
        plt.subplot(1, 2, 2)
        sns.histplot(assistant_lengths, bins=50)
        plt.title('Distribuição de Comprimento - Mensagens do Assistente')
        plt.xlabel('Comprimento (caracteres)')
        plt.ylabel('Contagem')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'message_length_distributions.png')
        plt.close()
        
        return {
            'user_mean': sum(user_lengths) / len(user_lengths),
            'user_max': max(user_lengths),
            'assistant_mean': sum(assistant_lengths) / len(assistant_lengths),
            'assistant_max': max(assistant_lengths)
        }
        
    def analyze_conversation_structure(self):
        """Analisa a estrutura das conversas."""
        turns_per_conversation = []
        roles_sequence = []
        
        for item in tqdm(self.data, desc="Analisando estrutura"):
            turns = len(item['messages'])
            turns_per_conversation.append(turns)
            roles_sequence.append([msg['role'] for msg in item['messages']])
            
        plt.figure(figsize=(10, 6))
        sns.histplot(turns_per_conversation, bins=range(min(turns_per_conversation), max(turns_per_conversation) + 2, 1))
        plt.title('Distribuição de Turnos por Conversa')
        plt.xlabel('Número de Turnos')
        plt.ylabel('Contagem')
        plt.savefig(self.output_dir / 'conversation_structure.png')
        plt.close()
        
        return {
            'mean_turns': sum(turns_per_conversation) / len(turns_per_conversation),
            'max_turns': max(turns_per_conversation),
            'min_turns': min(turns_per_conversation)
        }
        
    def generate_report(self):
        """Gera relatório completo da análise."""
        length_stats = self.analyze_message_lengths()
        structure_stats = self.analyze_conversation_structure()
        
        report = [
            "=== Relatório de Análise dos Dados de Treinamento ===\n",
            f"Total de conversas analisadas: {len(self.data)}",
            "\nEstatísticas de Comprimento:",
            f"- Média de caracteres (usuário): {length_stats['user_mean']:.2f}",
            f"- Máximo de caracteres (usuário): {length_stats['user_max']}",
            f"- Média de caracteres (assistente): {length_stats['assistant_mean']:.2f}",
            f"- Máximo de caracteres (assistente): {length_stats['assistant_max']}",
            "\nEstatísticas de Estrutura:",
            f"- Média de turnos por conversa: {structure_stats['mean_turns']:.2f}",
            f"- Máximo de turnos: {structure_stats['max_turns']}",
            f"- Mínimo de turnos: {structure_stats['min_turns']}"
        ]
        
        with open(self.output_dir / 'analysis_report.txt', 'w', encoding='utf-8') as f:
            f.write('\n'.join(report))
            
def main():
    """Função principal."""
    # Verifica se o arquivo de dados existe
    data_path = 'data/dataset.jsonl'
    if not os.path.exists(data_path):
        print(f"Erro: Arquivo {data_path} não encontrado!")
        return
        
    try:
        analyzer = TrainingDataAnalyzer(data_path)
        analyzer.generate_report()
        print(f"\nAnálise concluída! Resultados salvos em: {analyzer.output_dir}")
    except Exception as e:
        print(f"Erro durante a análise: {str(e)}")
        
if __name__ == '__main__':
    main() 