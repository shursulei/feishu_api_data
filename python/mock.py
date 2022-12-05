from config import KEY_SUBMIT, KEY_GREY, KEY_GA, KEY_STARTUP, KEY_VERSION
schema = {
    "version": 1,
    "proposal": 5,
    "delivery": 5,
    "expected": 5,
    "online": 5,
}

name_map = {
    "version": KEY_VERSION,
    "proposal": KEY_STARTUP,
    "delivery": KEY_SUBMIT,
    "expected": KEY_GREY,
    "online": KEY_GA,
}

raw_data = [
    {
        "version": "1.0.0",
        "proposal": '2022-07-20',
        "delivery": '2022-7-27',
        "expected": '2022-07-28',
        "online": '2022-07-29',
    },
    {
        "version": "1.1.0",
        "proposal": '2022-07-30',
        "delivery": '2022-08-04',
        "expected": '2022-08-05',
        "online": '2022-08-07',
    },
    {
        "version": "1.2.0",
        "proposal": '2022-08-09',
        "delivery": '2022-08-16',
        "expected": '2022-08-16',
        "online": '2022-08-16',
    }
]