import logging
import os
import time
import uuid
import config


class SimpleLogger:
    def __init__(self, name, level=logging.INFO, log_prefix='log'):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # ログのフォーマットを設定
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # コンソールハンドラの設定
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)

        current_unix_time = int(time.time())
        generated_uuid = uuid.uuid4().hex
        log_file = f'{log_prefix}_{name}_{current_unix_time}_{generated_uuid}.log'
        # ファイルハンドラの設定
        log_path = config.LOG_PATH
        log_file_path = os.path.join(log_path, log_file)
        file_handler = logging.FileHandler(log_file_path)
        file_handler.setFormatter(formatter)

        # 重複したハンドラが追加されないようにする
        self.logger.handlers.clear()

        # ハンドラをロガーに追加
        self.logger.addHandler(console_handler)
        self.logger.addHandler(file_handler)
        
    
    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)

# ロガーの使用例
if __name__ == "__main__":
    logger = SimpleLogger(__name__, logging.DEBUG)
    logger.debug("デバッグ情報")
    logger.info("情報ログ")
    logger.warning("警告メッセージ")
    logger.error("エラーメッセージ")
    logger.critical("重大なエラー")
