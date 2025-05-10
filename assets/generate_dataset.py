from argparse import ArgumentParser
import polars as pl
import numpy as np
import os
from datetime import datetime, timedelta


def generate_random_amount(
    mean: float,
    std: float = 500.0,
    min_val: float = 100.0,
    max_val: float = 10000.0,
) -> int:
    return int(round(max(min_val, min(max_val, np.random.normal(mean, std))), -1))


def main(n_rows: int, n_sale_rows: int, dest_dir: str, seed: int) -> None:
    # パラメータの設定
    np.random.seed(seed)
    start_date: datetime = datetime(2025, 1, 1)
    end_date: datetime = datetime(2025, 3, 31)
    date_range = (end_date - start_date).days
    hours: list[int] = list(range(9, 22))  # 9 to 21
    users: list[str] = [chr(i) for i in range(65, 91)]  # A to Z

    # ランダムなデータの生成
    random_days = np.random.randint(0, date_range + 1, n_rows)
    dates = [start_date + timedelta(days=int(d)) for d in random_days]
    random_hours = np.random.choice(hours, n_rows)
    random_users = np.random.choice(users, n_rows)

    # ユーザーごとに異なる平均値を持つランダムな購入金額の生成
    user_means = {user: np.random.uniform(1000, 5000) for user in users}
    amounts = [generate_random_amount(user_means[user]) for user in random_users]

    # メインのDataFrameの作成
    df = pl.DataFrame(
        {
            "売上日": [d.strftime("%Y-%m-%d") for d in dates],
            "時刻": random_hours,
            "ユーザー": random_users,
            "購入金額": amounts,
        }
    )
    df = df.sort(["売上日", "時刻", "ユーザー"])
    df.write_csv(os.path.join(dest_dir, "purchase_data.csv"))

    # ランダムなタイムセールテーブルの生成
    random_sale_dates = np.random.randint(0, date_range + 1, n_sale_rows)
    random_sale_hours = np.random.choice(hours, n_sale_rows)
    sale_dates = [
        start_date + timedelta(days=int(d), hours=int(h))
        for d, h in zip(random_sale_dates, random_sale_hours)
    ]
    sale_df = pl.DataFrame({"日時": sale_dates})
    sale_df.write_csv(os.path.join(dest_dir, "sale_data.csv"))


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--n_rows", type=int, default=1000)
    parser.add_argument("--n_sale_rows", type=int, default=10)
    parser.add_argument("--dest_dir", type=str, default="assets")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    print(f"Generating {args.n_rows} rows of sales data...")
    main(args.n_rows, args.n_sale_rows, args.dest_dir, args.seed)
    print(f"Saved to '{args.dest_dir}'!")
