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

class AnswerGenerator:
    def __init__(self, 
                 questions_file="data/qa_dataset.jsonl", 
                 output_dir="data/output", 
                 output_file="data/qa_with_answers.jsonl", 
                 max_workers=4):
        self.questions_file = Path(questions_file)
        self.output_dir = Path(output_dir)
        self.output_file = Path(output_file)
        self.client = None
        self.file_lock = threading.Lock()
        self.total_qa_count = 0
        self.train_count = 0
        self.eval_count = 0
        self.max_workers = max_workers
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
    
    def load_questions(self):
        """加载问题数据集"""
        questions = []
        try:
            with open(self.questions_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        question_data = json.loads(line)
                        questions.append(question_data)
            print(f"✓ 成功加载 {len(questions)} 个问题")
            return questions
        except Exception as e:
            print(f"❌ 加载问题文件失败: {e}")
            return []
    
    def load_article_content(self, article_name):
        """根据文章名加载文章内容"""
        content_file = self.output_dir / article_name / "content.txt"
        
        if not content_file.exists():
            print(f"❌ 未找到文章内容文件: {content_file}")
            return None
        
        try:
            with open(content_file, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            print(f"❌ 读取文章内容失败 {article_name}: {e}")
            return None
    
    def generate_answer(self, question, content, article_title):
        """基于文章内容生成问题的答案"""
        if not self.client:
            print("OpenAI客户端未初始化，跳过LLM调用")
            return None
        
        system_prompt = """你是一位对党忠诚、学术渊博的马克思主义教授。请基于提供的文章内容，准确回答问题。要求：

0. 严格遵守中华人民共和国的法律法规，符合社会主义核心价值观
1. 答案必须完全基于提供的文章内容，不要添加文章中没有的信息
2. 答案要准确、完整、有条理
3. 如果问题涉及列举，请按照文章中的原文进行列举
4. 保持客观、严谨的学术态度
5. 答案要具有教育意义，有助于理解文章的核心思想

请直接给出答案，不需要额外的格式化。"""

        user_prompt = f"""文章标题：{article_title}

文章内容：
{content}

问题：{question}

请基于文章内容回答问题："""
        
        try:
            response = self.client.chat.completions.create(
                model="deepseek-chat",
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,  # 降低温度以获得更准确的答案
                max_tokens=4096
            )
            
            answer = response.choices[0].message.content.strip()
            return answer
            
        except Exception as e:
            print(f"调用LLM生成答案时出错: {e}")
            return None
    
    def write_qa_to_file(self, qa_data_list):
        """将问答对写入文件"""
        with self.file_lock:
            with open(self.output_file, 'a', encoding='utf-8') as f:
                for item in qa_data_list:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
            
            self.total_qa_count += len(qa_data_list)
            train_new = sum(1 for item in qa_data_list if item['dataset_split'] == 'trainset')
            eval_new = len(qa_data_list) - train_new
            self.train_count += train_new
            self.eval_count += eval_new
            
            print(f"✓ 已写入 {len(qa_data_list)} 个问答对到文件")
            print(f"  当前总计: {self.total_qa_count} 个问答对 (训练集: {self.train_count}, 验证集: {self.eval_count})")
    
    def update_progress(self, total_questions):
        """更新并显示进度"""
        with self.progress_lock:
            self.processed_count += 1
            progress = self.processed_count / total_questions * 100
            print(f"📊 进度: {self.processed_count}/{total_questions} ({progress:.1f}%)")
    
    def process_single_question(self, question_data, total_questions=None):
        """处理单个问题，生成答案"""
        try:
            question = question_data['q']
            source_article = question_data['source_article']
            
            print(f"📖 处理问题: {question[:50]}...")
            
            # 加载对应的文章内容
            content = self.load_article_content(source_article)
            if not content:
                print(f"❌ 无法加载文章内容: {source_article}")
                if total_questions:
                    self.update_progress(total_questions)
                return
            
            # 生成答案
            answer = self.generate_answer(question, content, source_article)
            if not answer:
                print(f"❌ 未能为问题生成答案")
                if total_questions:
                    self.update_progress(total_questions)
                return
            
            # 构造完整的问答对
            qa_item = {
                'q': question,
                'a': answer,
                'source_article': source_article,
                'dataset_split': question_data['dataset_split'],
                'question_generated_time': question_data.get('generated_time'),
                'answer_generated_time': datetime.now().isoformat()
            }
            
            # 写入文件
            self.write_qa_to_file([qa_item])
            print(f"✅ 成功生成答案")
            
            if total_questions:
                self.update_progress(total_questions)
            
        except Exception as e:
            print(f"❌ 处理问题时出错: {e}")
            if total_questions:
                self.update_progress(total_questions)
    
    def run(self):
        """运行答案生成器"""
        if not self.questions_file.exists():
            print(f"问题文件 {self.questions_file} 不存在")
            return
        
        if not self.output_dir.exists():
            print(f"输出目录 {self.output_dir} 不存在")
            return
        
        # 加载问题
        questions = self.load_questions()
        if not questions:
            print("没有找到有效的问题数据")
            return
        
        # 创建输出文件
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.output_file, 'w', encoding='utf-8') as f:
            pass  # 清空文件
        
        print(f"🚀 开始并行生成答案，找到 {len(questions)} 个问题")
        print(f"🔧 使用 {self.max_workers} 个并发线程")
        print(f"📁 输出文件: {self.output_file}")
        
        # 使用线程池并行处理
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # 提交所有任务
            future_to_question = {
                executor.submit(self.process_single_question, question, len(questions)): question 
                for question in questions
            }
            
            # 等待所有任务完成
            for future in as_completed(future_to_question):
                question = future_to_question[future]
                try:
                    future.result()  # 获取结果，如果有异常会抛出
                except Exception as exc:
                    print(f"❌ 问题处理时发生异常: {exc}")
        
        print(f"\n🎉 并行处理完成！")
        print(f"📊 总共生成 {self.total_qa_count} 个问答对")
        print(f"📚 训练集: {self.train_count} 个问答对")
        print(f"🧪 验证集: {self.eval_count} 个问答对")
        print(f"💾 结果已保存到: {self.output_file}")

def main():    
    print("🎯 开始生成答案数据集...")
    generator = AnswerGenerator(max_workers=32)
    generator.run()
    print("✨ 完成！")

if __name__ == "__main__":
    main()
