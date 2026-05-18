import requests

strategy_cache = {}

API_URL = "https://algoapi.dreamintraders.in/api/deployments/user/today/all"

def load_users(strategy_id):
    try:
        res = requests.get(API_URL)
        data = res.json()

        grouped = {}

        for user in data:
            if user.get("type") == "paper":
                continue

            if user.get("strategy_id") != strategy_id:
                continue

            broker = user.get("broker_name","None")

            grouped.setdefault(broker, []).append({
                "user_id": user.get("user_id"),
                "multiplier": user.get("multiplier", 1),
                "credentials": user.get("credentials", {})
            })
        strategy_cache[strategy_id] = grouped

        print(f"✅ Loaded users for {strategy_id}")

        return grouped

    except Exception as e:
        print("❌ Error loading users:", e)
        return {}