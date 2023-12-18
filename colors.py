class Colors:
    PINK = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END_COLOR = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

    type_to_colors = {
        "vote": BLUE,
        "append": GREEN,
        "role": CYAN,
        "connect": PINK,
        "error": RED,
        "warning": YELLOW
    }


def colorful_print(to_print: str, msg_type: str):
    print(Colors.type_to_colors[msg_type] + to_print + Colors.END_COLOR)
