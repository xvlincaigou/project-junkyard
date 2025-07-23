import torch
from unsloth import FastLanguageModel
from transformers import TrainingArguments, TrainerCallback, Trainer, TrainingArguments, TrainerState, TrainerControl
from trl import SFTTrainer
from datasets import Dataset
import wandb
import os
import json
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from openai import OpenAI
from pathlib import Path

os.environ["WANDB_PROJECT"] = "MaoWen"
os.environ["WANDB_MODE"] = "offline"

JUDGE_PROMPT = """
请根据以下问题和学生的回答，给学生的回答打分。

问题：{question}
学生的回答：{answer}

请根据以下标准给学生的回答打分：
9～10分：回答正确且有深度思考。
7～8分：回答正确但缺乏深度思考。
5～6分：回答有错误。
3～4分：回答严重错误。
1～2分：回答与问题无关。

请直接给出你的打分，不要给出任何其他内容，只给出数字:
"""

def judge(question: str, answer: str):
    """
    Sends the question and answer to the DeepSeek API for judging.
    Returns the score as an integer.
    """
    client = OpenAI(api_key="sk-1040a650df194c1485c95027c4bdc7fb", base_url="https://api.deepseek.com")
    prompt = JUDGE_PROMPT.format(question=question, answer=answer)
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",
            messages=[
                {"role": "system", "content": "你是一位对党忠诚、学术渊博的马克思主义教授。"},
                {"role": "user", "content": prompt},
            ],
            stream=False,
            timeout=30,
        )
        return int(response.choices[0].message.content)
    except Exception as e:
        print(f"An error occurred during judging: {e}")
        return 0

def load_dataset():
    """Loads the full dataset and splits it into train and eval sets."""
    try:
        with open("data/qa_with_answers.jsonl", "r", encoding="utf-8") as f:
            full_data = [json.loads(line) for line in f]
    except FileNotFoundError:
        print("Error: data/qa_with_answers.jsonl not found. Please make sure the dataset exists.")
        return [], []
    train_data = [d for d in full_data if d.get("dataset_split") == "trainset"]
    eval_data = [d for d in full_data if d.get("dataset_split") == "evalset"]
    return train_data, eval_data

def formatting_train_func(original_train_data, tokenizer):
    """Formats the training data with both question and answer."""
    from unsloth.chat_templates import get_chat_template
    tokenizer = get_chat_template(tokenizer, chat_template="qwen2.5")
    return list(map(lambda qa_pair: {
        "text": tokenizer.apply_chat_template([
            {"role": "user", "content": qa_pair["q"]},
            {"role": "assistant", "content": qa_pair["a"]}
        ], tokenize=False)
    }, original_train_data))

def evaluate_checkpoint(checkpoint_path: str, eval_data: list, global_step: int):
    """
    Loads a checkpoint, generates predictions, saves to a file, judges, and logs to W&B.
    This function mimics the logic of the original eval.py script.
    """
    print(f"\n--- Running evaluation for checkpoint at step {global_step} ---")
    output_file = Path("eval_outputs.jsonl")
    
    try:
        model, tokenizer = FastLanguageModel.from_pretrained(
            model_name=checkpoint_path,
            max_seq_length=2048,
            dtype=None,
            load_in_4bit=False,
        )
        
        prompts = [
            tokenizer.apply_chat_template(
                [{'role': 'user', 'content': item["q"]}],
                tokenize=False,
                add_generation_prompt=True
            ) for item in eval_data
        ]
        
        generated_answers = []
        for i in tqdm(range(0, len(prompts), 8), desc="Generating answers"):
             batch_prompts = prompts[i:i+8]
             inputs = tokenizer(batch_prompts, return_tensors="pt", padding=True).to("cuda")
             outputs = model.generate(**inputs, max_new_tokens=1024, use_cache=True)
             batch_answers = tokenizer.batch_decode(outputs, skip_special_tokens=True)
             cleaned_answers = [ans.replace(prompt, "").strip() for ans, prompt in zip(batch_answers, batch_prompts)]
             generated_answers.extend(cleaned_answers)


        with open(output_file, "w", encoding="utf-8") as f:
            for item, answer in zip(eval_data, generated_answers):
                item["qwen_answer"] = answer
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        print(f"Predictions saved to {output_file}")

    except Exception as e:
        print(f"An error occurred during prediction for checkpoint {checkpoint_path}: {e}")
        return

    # 2. Judge Step
    try:
        with open(output_file, "r", encoding="utf-8") as f:
            judging_data = [json.loads(line) for line in f]

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(judge, item["q"], item["qwen_answer"]) for item in judging_data]
            scores = [future.result() for future in tqdm(futures, desc="Judging")]
        
        avg_score = sum(scores) / len(scores) if scores else 0
        print(f"Average Judge Score for step {global_step}: {avg_score:.4f}")

        # 3. Log to W&B
        wandb.log({"eval/average_judge_score": avg_score}, step=global_step)
        print("--- Evaluation complete ---")

    except Exception as e:
        print(f"An error occurred during judging for checkpoint {checkpoint_path}: {e}")


class CheckpointEvalCallback(TrainerCallback):
    def __init__(self, eval_data: list):
        self.eval_data = eval_data

    def on_save(self, args: TrainingArguments, state: TrainerState, control: TrainerControl, **kwargs):
        """Event triggered after a checkpoint is saved."""
        checkpoint_folder = os.path.join(args.output_dir, f"checkpoint-{state.global_step}")
        if os.path.exists(checkpoint_folder):
            evaluate_checkpoint(
                checkpoint_path=checkpoint_folder,
                eval_data=self.eval_data,
                global_step=state.global_step
            )

if __name__ == "__main__":
    # 1. 加载模型和分词器 (Load model and tokenizer)
    model_name = "./Qwen2.5-0.5B-Instruct"
    max_seq_length = 2048
    dtype = None
    load_in_4bit = False

    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name = model_name,
        max_seq_length = max_seq_length,
        dtype = dtype,
        load_in_4bit = load_in_4bit,
    )
    # Set a padding token if one is not set
    if tokenizer.pad_token is None:
        tokenizer.pad_token = tokenizer.eos_token

    # 2. 为模型添加 Lora 适配器 (Add LoRA adapters to model)
    model = FastLanguageModel.get_peft_model(
        model,
        r=32,
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"],
        lora_alpha=32,
        lora_dropout=0,
        bias="none",
        use_gradient_checkpointing=True,
        random_state=3407,
        use_rslora=False,
        loftq_config=None,
    )

    # 3. 准备您的数据集 (Prepare your datasets)
    original_train_data, original_eval_data = load_dataset()
    train_dataset = Dataset.from_list(formatting_train_func(original_train_data, tokenizer))

    eval_callback = CheckpointEvalCallback(eval_data=original_eval_data)

    # 4. 配置训练参数并开始训练 (Configure training arguments and start training)
    training_args = TrainingArguments(
        per_device_train_batch_size=32,
        gradient_accumulation_steps=4,
        warmup_steps=5,
        max_steps=750,
        learning_rate=1e-5,
        fp16=not torch.cuda.is_bf16_supported(),
        bf16=torch.cuda.is_bf16_supported(),
        logging_steps=1,
        optim="adamw_8bit",
        weight_decay=0.01,
        lr_scheduler_type="linear",
        seed=3407,
        output_dir="outputs",
        report_to="wandb",
        run_name="qwen2.5-0.5b-lora-file-eval-run-1",
        save_strategy="steps",
        save_steps=150,
    )

    trainer = SFTTrainer(
        model=model,
        tokenizer=tokenizer,
        train_dataset=train_dataset,
        packing=False,
        max_seq_length=max_seq_length,
        args=training_args,
        callbacks=[eval_callback],
    )

    print("Starting training with periodic file-based evaluation...")
    trainer.train()
    print("Training finished.")

    wandb.finish()

    print("Saving final LoRA model...")
    model.save_pretrained("lora_model")
    print("Model saved to lora_model/")
