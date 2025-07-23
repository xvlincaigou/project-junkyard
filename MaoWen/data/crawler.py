#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
毛泽东文章爬虫
从 https://www.marxists.org/chinese/maozedong/index.htm 爬取所有文章
"""

import requests
from bs4 import BeautifulSoup
import os
import time
import re
from urllib.parse import urljoin, urlparse
import json
import logging
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

class MaoZedongCrawler:
    def __init__(self, base_url="https://www.marxists.org/chinese/maozedong/index.htm"):
        self.base_url = base_url
        self.base_domain = "https://www.marxists.org"
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        })
        self.output_dir = "output"
        self.articles_info = []
        
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
    
    def get_page(self, url, max_retries=3):
        """获取网页内容，带重试机制"""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, timeout=30)
                # 检测编码格式，优先尝试GB2312
                if 'charset=' in response.headers.get('content-type', '').lower():
                    # 如果响应头中指定了编码，使用指定的编码
                    pass  # requests会自动处理
                else:
                    # 尝试不同的编码格式
                    try:
                        # 先尝试GB2312/GBK编码
                        response.encoding = 'gb2312'
                        test_text = response.text[:1000]  # 测试前1000个字符
                        if '毛泽东' in test_text or '选集' in test_text:
                            logging.info("使用GB2312编码")
                        else:
                            response.encoding = 'utf-8'
                            logging.info("使用UTF-8编码")
                    except:
                        response.encoding = 'utf-8'
                
                if response.status_code == 200:
                    return response
                else:
                    logging.warning(f"HTTP {response.status_code} for {url}")
            except Exception as e:
                logging.warning(f"Attempt {attempt + 1} failed for {url}: {e}")
                time.sleep(2)
        
        logging.error(f"Failed to get {url} after {max_retries} attempts")
        return None
    
    def clean_filename(self, filename):
        """清理文件名，移除非法字符"""
        # 移除或替换非法字符
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = filename.strip()
        # 限制文件名长度
        if len(filename) > 200:
            filename = filename[:200]
        return filename
    
    def extract_article_links(self):
        """从主页面提取所有文章链接"""
        logging.info("开始提取文章链接...")
        
        response = self.get_page(self.base_url)
        if not response:
            logging.error("无法获取主页面")
            return []
        
        soup = BeautifulSoup(response.text, 'html.parser')
        article_links = []
        
        # 查找所有的文章链接
        # 记录页面内容用于调试
        logging.info(f"页面标题: {soup.title.get_text() if soup.title else '无标题'}")
        
        # 标记是否已经到达第五卷
        reached_volume_five = False
        
        # 如果没有通过表格找到第五卷标记，则通过其他方式检查
        if not reached_volume_five:
            # 模式2: 直接查找所有链接，但需要按顺序检查
            page_text = soup.get_text()
            fifth_volume_pos = -1
            
            # 查找第五卷的位置标记
            fifth_volume_markers = ['有学者指出，1977年官方版《毛泽东选集》']
            for marker in fifth_volume_markers:
                pos = page_text.find(marker)
                if pos != -1:
                    fifth_volume_pos = pos
                    logging.info(f"找到第五卷标记: {marker}")
                    break
            
            all_links = soup.find_all('a', href=True)
            for link in all_links:
                # 如果找到了第五卷位置，检查链接是否在第五卷之后
                if fifth_volume_pos != -1:
                    link_text_pos = page_text.find(link.get_text().strip())
                    if link_text_pos > fifth_volume_pos:
                        continue  # 跳过第五卷之后的链接
                
                href = link.get('href')
                if href and href.endswith('.htm') and not href.endswith('index.htm'):
                    # 构建完整URL
                    if href.startswith('/'):
                        full_url = self.base_domain + href
                    elif href.startswith('http'):
                        full_url = href
                    else:
                        full_url = urljoin(self.base_url, href)
                    
                    title = link.get_text().strip()
                    
                    # 额外的过滤：跳过明显是第五卷之后的内容
                    if any(keyword in title for keyword in ['思想万岁', '1949年', '1950年', '1951年', '1952年', '1953年', '1954年', '1955年', '1956年', '1957年']):
                        continue
                    
                    if title and len(title) > 1:
                        article_links.append({
                            'title': title,
                            'url': full_url,
                            'href': href
                        })
        
        # 去重
        seen_urls = set()
        unique_links = []
        for link in article_links:
            if link['url'] not in seen_urls:
                seen_urls.add(link['url'])
                unique_links.append(link)
        
        logging.info(f"共找到 {len(unique_links)} 个文章链接（已过滤第五卷及之后内容）")
        return unique_links
    
    def download_article(self, article_info, index):
        """下载单篇文章"""
        title = article_info['title']
        url = article_info['url']
        
        logging.info(f"正在下载第 {index + 1} 篇文章: {title}")
        
        response = self.get_page(url)
        if not response:
            logging.error(f"无法下载文章: {title}")
            return None
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 提取文章内容
        content = ""
        
        # 尝试不同的内容选择器
        content_selectors = [
            'div.content',
            'div#content',
            'article',
            'div.main',
            'div.text',
            'body'
        ]
        
        article_content = None
        for selector in content_selectors:
            article_content = soup.select_one(selector)
            if article_content:
                break
        
        if not article_content:
            article_content = soup
        
        # 移除导航和非内容元素
        for element in article_content.find_all(['nav', 'header', 'footer', 'script', 'style']):
            element.decompose()
        
        # 提取文本内容
        content = article_content.get_text().strip()
        
        # 清理内容
        content = re.sub(r'\n\s*\n', '\n\n', content)  # 合并多个空行
        content = re.sub(r'[ \t]+', ' ', content)  # 合并多个空格
        
        if len(content) < 100:  # 内容太短，可能不是正文
            logging.warning(f"文章内容太短，可能提取失败: {title}")
        
        # 保存文章
        filename = self.clean_filename(f"{index:03d}_{title}")
        article_dir = os.path.join(self.output_dir, filename)
        os.makedirs(article_dir, exist_ok=True)
        
        # 保存文本内容
        txt_file = os.path.join(article_dir, "content.txt")
        with open(txt_file, 'w', encoding='utf-8') as f:
            f.write(f"标题: {title}\n")
            f.write(f"链接: {url}\n")
            f.write(f"下载时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("-" * 50 + "\n\n")
            f.write(content)
        
        # 保存HTML源码
        html_file = os.path.join(article_dir, "source.html")
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        
        article_data = {
            'index': index,
            'title': title,
            'url': url,
            'filename': filename,
            'content_length': len(content),
            'download_time': datetime.now().isoformat()
        }
        
        self.articles_info.append(article_data)
        
        logging.info(f"已保存文章: {title} ({len(content)} 字符)")
        return article_data
    
    def save_articles_index(self):
        """保存文章索引"""
        index_file = os.path.join(self.output_dir, "articles_index.json")
        with open(index_file, 'w', encoding='utf-8') as f:
            json.dump(self.articles_info, f, ensure_ascii=False, indent=2)
        
        # 也保存一个可读的文本版本
        txt_index_file = os.path.join(self.output_dir, "articles_index.txt")
        with open(txt_index_file, 'w', encoding='utf-8') as f:
            f.write("毛泽东文章索引\n")
            f.write("=" * 50 + "\n\n")
            for article in self.articles_info:
                f.write(f"{article['index']:03d}. {article['title']}\n")
                f.write(f"     链接: {article['url']}\n")
                f.write(f"     文件: {article['filename']}\n")
                f.write(f"     字数: {article['content_length']}\n")
                f.write(f"     下载时间: {article['download_time']}\n\n")
    
    def crawl_all(self):
        """爬取所有文章"""
        logging.info("开始爬取毛泽东文章...")
        
        # 获取所有文章链接
        article_links = self.extract_article_links()
        if not article_links:
            logging.error("未找到任何文章链接")
            return
        
        logging.info(f"准备下载 {len(article_links)} 篇文章")
        
        # 下载每篇文章
        for i, article_info in enumerate(article_links):
            try:
                self.download_article(article_info, i)
                # 添加延迟避免过于频繁的请求
                time.sleep(1)
            except Exception as e:
                logging.error(f"下载文章失败: {article_info['title']}, 错误: {e}")
                continue
        
        # 保存索引
        self.save_articles_index()
        
        logging.info(f"爬取完成！共成功下载 {len(self.articles_info)} 篇文章")
        logging.info(f"文章保存在目录: {self.output_dir}")

def main():
    """主函数"""
    crawler = MaoZedongCrawler()
    crawler.crawl_all()

if __name__ == "__main__":
    main()
