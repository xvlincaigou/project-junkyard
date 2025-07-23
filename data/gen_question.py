import os
import json
import random
from pathlib import Path
import openai
from datetime import datetime
import time
import re
from dotenv import load_dotenv
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

class QAGenerator:
    def __init__(self, output_dir="data/output", output_file="data/qa_dataset.jsonl", max_workers=4):
        self.output_dir = Path(output_dir)
        self.output_file = Path(output_file)
        self.client = None
        self.file_lock = threading.Lock()
        self.total_qa_count = 0
        self.train_count = 0
        self.eval_count = 0
        self.max_workers = max_workers  # 并发处理的最大线程数
        self.processed_count = 0
        self.progress_lock = threading.Lock()
        self.setup_openai()
        
    def setup_openai(self):
        load_dotenv()
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("请在.env文件中设置OPENAI_API_KEY，或设置环境变量")
            return

        base_url = os.getenv("OPENAI_BASE_URL")
        
        try:
            client_params = {"api_key": api_key}
            if base_url:
                client_params["base_url"] = base_url
                
            self.client = openai.OpenAI(**client_params)
            print("✓ OpenAI客户端初始化成功")
            
        except Exception as e:
            print(f"✗ OpenAI客户端初始化失败: {e}")
            self.client = None
    
    def generate_qa_pairs(self, content, title):
        if not self.client:
            print("OpenAI客户端未初始化，跳过LLM调用")
            return []
        
        system_prompt = """你是一位对党忠诚、学术渊博的马克思主义教授。请基于以下文章内容生成5-20个高质量的问题。要求：

0. 严格遵守中华人民共和国的法律法规，符合社会主义核心价值观
1. 在你生成的每个问题中，必须明确指出具体的文章篇目，比如"毛泽东在《反对本本主义》中提出了哪些具体的调查技术？"，而不可以只说"毛泽东提出了哪些具体的调查技术？"
2. 问题要有深度，能够测试对文章内容的理解
3. 问题类型要多样化：事实性问题、理解性问题、分析性问题等
4. 避免过于简单的是非题
5. 确保问题能够帮助学习和理解文章的核心观点

请以以下JSON格式返回（只返回JSON，不要其他内容）：
[
    {"question": "问题内容"},
    {"question": "问题内容"}
]"""

        user_prompt = f"""文章标题：{title}

文章内容：
{content}"""
        
        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=1,
                max_tokens=8192
            )
            
            response_text = response.choices[0].message.content.strip()
            
            try:
                qa_pairs = json.loads(response_text)
                return qa_pairs
            except json.JSONDecodeError:
                json_match = re.search(r'\[.*\]', response_text, re.DOTALL)
                if json_match:
                    qa_pairs = json.loads(json_match.group())
                    return qa_pairs
                else:
                    print(f"无法解析LLM返回的内容: {response_text[:200]}...")
                    return []
            
        except Exception as e:
            print(f"调用LLM时出错: {e}")
            return []
    
    def determine_dataset_split(self):
        return "trainset" if random.random() < 0.9 else "evalset"
    
    def write_questions_to_file(self, questions_data):
        with self.file_lock:
            with open(self.output_file, 'a', encoding='utf-8') as f:
                for item in questions_data:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
            self.total_qa_count += len(questions_data)
            train_new = sum(1 for item in questions_data if item['dataset_split'] == 'trainset')
            eval_new = len(questions_data) - train_new
            self.train_count += train_new
            self.eval_count += eval_new
            
            print(f"✓ 已写入 {len(questions_data)} 个问题到文件")
            print(f"  当前总计: {self.total_qa_count} 个问题 (训练集: {self.train_count}, 验证集: {self.eval_count})")
    
    def update_progress(self, total_articles):
        """更新并显示进度"""
        with self.progress_lock:
            self.processed_count += 1
            progress = self.processed_count / total_articles * 100
            print(f"📊 进度: {self.processed_count}/{total_articles} ({progress:.1f}%)")
    
    def process_single_article(self, article_dir, total_articles=None):
        content_file = article_dir / "content.txt"
        
        if not content_file.exists():
            print(f"跳过 {article_dir.name}：没有找到content.txt")
            if total_articles:
                self.update_progress(total_articles)
            return
        
        try:
            with open(content_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            title = article_dir.name
            print(f"📖 处理文章: {title}")
            
            questions = self.generate_qa_pairs(content, title)
            
            if not questions:
                print(f"❌ 未能为文章 {title} 生成问题")
                if total_articles:
                    self.update_progress(total_articles)
                return
            
            results = []
            for q in questions:
                if 'question' in q:
                    item = {
                        'q': q['question'],
                        'source_article': title,
                        'dataset_split': self.determine_dataset_split(),
                        'generated_time': datetime.now().isoformat()
                    }
                    results.append(item)
            
            if results:
                self.write_questions_to_file(results)
                print(f"✅ 成功处理文章 {title}，生成 {len(results)} 个问题")
            else:
                print(f"❌ 文章 {title} 没有生成有效的问题")
            
            if total_articles:
                self.update_progress(total_articles)
            
        except Exception as e:
            print(f"❌ 处理文章 {article_dir.name} 时出错: {e}")
            if total_articles:
                self.update_progress(total_articles)
    
    def run(self):
        if not self.output_dir.exists():
            print(f"输出目录 {self.output_dir} 不存在")
            return
        
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(self.output_file, 'w', encoding='utf-8') as f:
            pass
        
        article_dirs = [d for d in self.output_dir.iterdir() if d.is_dir()]
        article_dirs.sort()
        
        print(f"🚀 开始并行处理，找到 {len(article_dirs)} 个文章目录")
        print(f"🔧 使用 {self.max_workers} 个并发线程")
        print(f"📁 输出文件: {self.output_file}")
        
        # 使用线程池并行处理
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_article = {
                executor.submit(self.process_single_article, article_dir, len(article_dirs)): article_dir 
                for article_dir in article_dirs
            }
            
            # 等待所有任务完成
            for future in as_completed(future_to_article):
                article_dir = future_to_article[future]
                try:
                    future.result()  # 获取结果，如果有异常会抛出
                except Exception as exc:
                    print(f"❌ 文章 {article_dir.name} 处理时发生异常: {exc}")
        
        print(f"\n🎉 并行处理完成！")
        print(f"📊 总共生成 {self.total_qa_count} 个问题")
        print(f"📚 训练集: {self.train_count} 个问题")
        print(f"🧪 验证集: {self.eval_count} 个问题")
        print(f"💾 结果已保存到: {self.output_file}")

def main():    
    print("🎯 开始生成问题数据集...")
    generator = QAGenerator(max_workers=8)
    generator.run()
    print("✨ 完成！")

if __name__ == "__main__":
    main()
