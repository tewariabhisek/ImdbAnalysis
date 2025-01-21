import logging

class Logger:
    def get_logger(self, log_file_path):
        logging.basicConfig(
            format='%(asctime)s - %(levelname)s - %(message)s',
            level=logging.INFO,
            handlers=[
                logging.FileHandler(log_file_path),
                logging.StreamHandler()
            ]
        )
        return logging

