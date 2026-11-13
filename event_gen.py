"""
Synthetic e-commerce clickstream/order event generator.

Simulates a pool of concurrent user sessions moving through a realistic
funnel (page_view -> product_view -> search/add_to_cart -> purchase),
so the resulting stream has properties a real Flink job would care about:

- Sessions with a bounded lifetime (good for session-window exercises)
- A funnel structure (good for conversion-rate / drop-off jobs)
- Repeated user_id / session_id / product_id keys (good for keyBy + state)
- Realistic skew: some products and some users are much more active than
  others (good for hot-key / skew-handling exercises later)
- A running cart snapshot on every event (good for cart-abandonment /
  real-time-personalization style jobs)

The event carries device, geo, and marketing-attribution context on every
record — the same categories of field real clickstream platforms
(Snowplow, Segment, Amplitude) send — which is also what naturally brings
a typical event up to roughly 1KB, rather than reaching that size with
meaningless padding.
"""
import random
import time
import uuid

CATEGORIES = ["electronics", "books", "home_kitchen", "clothing", "sports", "beauty"]

BRANDS = ["Nova", "Artisan", "Zenith", "Prime", "Wildwood", "Orbit", "Halcyon", "Redline"]
ADJECTIVES = ["Premium", "Compact", "Wireless", "Portable", "Classic", "Pro", "Eco", "Ultra"]
NOUNS = {
    "electronics": ["Headphones", "Charger", "Speaker", "Monitor", "Keyboard"],
    "books": ["Novel", "Cookbook", "Journal", "Guide", "Biography"],
    "home_kitchen": ["Blender", "Kettle", "Cookware Set", "Lamp", "Organizer"],
    "clothing": ["Jacket", "Sneakers", "T-Shirt", "Hoodie", "Scarf"],
    "sports": ["Yoga Mat", "Water Bottle", "Resistance Bands", "Backpack", "Gloves"],
    "beauty": ["Moisturizer", "Serum", "Shampoo", "Lip Balm", "Sunscreen"],
}
TAG_POOL = ["bestseller", "new_arrival", "limited_edition", "eco_friendly",
            "on_sale", "staff_pick", "trending", "restocked"]
DESCRIPTION_TEMPLATES = [
    "Designed for everyday use, this {noun_lower} combines durability with a "
    "clean, minimal look that fits any {category_label} collection.",
    "A customer favorite in our {category_label} lineup — thoughtfully built "
    "with premium materials and backed by a 2-year warranty.",
    "Lightweight, reliable, and built to last. This {noun_lower} is one of "
    "our top-rated picks in {category_label} this season.",
]

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [0.6, 0.35, 0.05]

OS_BY_DEVICE = {
    "mobile": ["iOS 18.1", "Android 15"],
    "desktop": ["Windows 11", "macOS 15.1", "Ubuntu 24.04"],
    "tablet": ["iPadOS 18.1", "Android 15"],
}
BROWSERS = ["Chrome 128", "Safari 18", "Firefox 130", "Edge 128"]
USER_AGENT_TEMPLATES = {
    "Chrome 128": "Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/128.0.0.0 Safari/537.36",
    "Safari 18": "Mozilla/5.0 ({platform}) AppleWebKit/605.1.15 (KHTML, like Gecko) "
                 "Version/18.0 Safari/605.1.15",
    "Firefox 130": "Mozilla/5.0 ({platform}) Gecko/20100101 Firefox/130.0",
    "Edge 128": "Mozilla/5.0 ({platform}) AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0",
}
SCREEN_RES_BY_DEVICE = {
    "mobile": ["390x844", "414x896", "412x915"],
    "desktop": ["1920x1080", "2560x1440", "1440x900"],
    "tablet": ["1024x1366", "820x1180"],
}

REFERRERS = ["direct", "search_engine", "social", "email", "ad_campaign"]
REFERRER_WEIGHTS = [0.25, 0.35, 0.2, 0.1, 0.1]
UTM_SOURCES = {
    "search_engine": ["google", "bing"],
    "social": ["instagram", "facebook", "tiktok"],
    "email": ["newsletter", "abandoned_cart_email"],
    "ad_campaign": ["google_ads", "meta_ads"],
}
UTM_CAMPAIGNS = ["summer_sale", "new_arrivals", "flash_deal", "loyalty_rewards", "clearance"]

COUNTRIES = ["US", "VN", "SG", "GB", "DE", "JP", "AU"]
COUNTRY_WEIGHTS = [0.35, 0.15, 0.1, 0.1, 0.1, 0.1, 0.1]
GEO_BY_COUNTRY = {
    "US": [("New York", "NY", 40.71, -74.01), ("Austin", "TX", 30.27, -97.74)],
    "VN": [("Ho Chi Minh City", "SG", 10.78, 106.70), ("Hanoi", "HN", 21.03, 105.85)],
    "SG": [("Singapore", "SG", 1.35, 103.82)],
    "GB": [("London", "ENG", 51.51, -0.13)],
    "DE": [("Berlin", "BE", 52.52, 13.40)],
    "JP": [("Tokyo", "13", 35.68, 139.69)],
    "AU": [("Sydney", "NSW", -33.87, 151.21)],
}

AB_VARIANTS = ["control", "variant_a", "variant_b"]

PLATFORM_STRINGS = {
    "iOS 18.1": "iPhone; CPU iPhone OS 18_1 like Mac OS X",
    "Android 15": "Linux; Android 15; SM-S928B",
    "Windows 11": "Windows NT 10.0; Win64; x64",
    "macOS 15.1": "Macintosh; Intel Mac OS X 15_1",
    "Ubuntu 24.04": "X11; Linux x86_64",
    "iPadOS 18.1": "iPad; CPU OS 18_1 like Mac OS X",
}
REFERRER_URL_TEMPLATES = {
    "search_engine": "https://www.{source}.com/search?q={query}",
    "social": "https://www.{source}.com/link/{trace}",
    "email": "https://mail.example.com/campaign/{campaign}",
    "ad_campaign": "https://ads.{source}.com/click?campaign={campaign}",
}

SEARCH_TERMS = [
    "wireless headphones", "running shoes", "coffee maker", "yoga mat",
    "laptop stand", "desk lamp", "phone case", "backpack",
    "bluetooth speaker", "water bottle",
]

# Funnel: each event type maps to what typically comes next, with weights.
FUNNEL_TRANSITIONS = {
    "start":        [("page_view", 1.0)],
    "page_view":    [("product_view", 0.55), ("search", 0.25), ("end", 0.20)],
    "search":       [("product_view", 0.6), ("page_view", 0.2), ("end", 0.2)],
    "product_view": [("add_to_cart", 0.35), ("product_view", 0.25),
                      ("page_view", 0.15), ("end", 0.25)],
    "add_to_cart":  [("purchase", 0.45), ("remove_from_cart", 0.15),
                      ("product_view", 0.20), ("end", 0.20)],
    "remove_from_cart": [("product_view", 0.5), ("end", 0.5)],
    "purchase":     [("end", 1.0)],
}


def _weighted_choice(pairs):
    options, weights = zip(*pairs)
    return random.choices(options, weights=weights, k=1)[0]


class ProductCatalog:
    """
    A fixed catalog with realistic popularity skew (a few bestsellers,
    a long tail) — makes hot-key / skewed-partition behavior visible.
    """

    def __init__(self, num_products: int = 200, seed: int | None = None):
        rng = random.Random(seed)
        self.products = []
        for i in range(num_products):
            category = rng.choice(CATEGORIES)
            noun = rng.choice(NOUNS[category])
            name = f"{rng.choice(ADJECTIVES)} {noun}"
            brand = rng.choice(BRANDS)
            tags = rng.sample(TAG_POOL, k=rng.randint(1, 3))
            price = round(rng.uniform(5, 500), 2)
            description = rng.choice(DESCRIPTION_TEMPLATES).format(
                noun_lower=noun.lower(), category_label=category.replace("_", " "))
            self.products.append({
                "product_id": f"p-{i:05d}",
                "name": name,
                "brand": brand,
                "category": category,
                "price": price,
                "tags": tags,
                "description": description,
                "image_url": f"https://cdn.shop.example.com/img/p-{i:05d}.jpg",
            })
        # Zipf-like popularity weights: a small number of products dominate views
        ranks = list(range(1, num_products + 1))
        self.weights = [1.0 / r for r in ranks]

    def sample(self) -> dict:
        return random.choices(self.products, weights=self.weights, k=1)[0]
		

class Session:
    __slots__ = ("session_id", "user_id", "device", "os", "browser", "user_agent",
                 "screen_resolution", "referrer", "referrer_url", "utm_source",
                 "utm_campaign", "country", "city", "region", "lat", "lon",
                 "ab_variant", "state", "steps", "last_product", "cart")

    def __init__(self, user_id: str, rng: random.Random):
        self.session_id = f"s-{uuid.uuid4().hex[:12]}"
        self.user_id = user_id

        self.device = _weighted_choice(list(zip(DEVICES, DEVICE_WEIGHTS)))
        self.os = rng.choice(OS_BY_DEVICE[self.device])
        self.browser = rng.choice(BROWSERS)
        platform = PLATFORM_STRINGS[self.os]
        self.user_agent = USER_AGENT_TEMPLATES[self.browser].format(platform=platform)
        self.screen_resolution = rng.choice(SCREEN_RES_BY_DEVICE[self.device])

        self.referrer = _weighted_choice(list(zip(REFERRERS, REFERRER_WEIGHTS)))
        if self.referrer in UTM_SOURCES:
            self.utm_source = rng.choice(UTM_SOURCES[self.referrer])
            self.utm_campaign = rng.choice(UTM_CAMPAIGNS)
            self.referrer_url = REFERRER_URL_TEMPLATES[self.referrer].format(
                source=self.utm_source, campaign=self.utm_campaign,
                query=rng.choice(SEARCH_TERMS).replace(" ", "+"),
                trace=uuid.uuid4().hex[:8],
            )
        else:
            self.utm_source = None
            self.utm_campaign = None
            self.referrer_url = None

        self.country = _weighted_choice(list(zip(COUNTRIES, COUNTRY_WEIGHTS)))
        self.city, self.region, self.lat, self.lon = rng.choice(GEO_BY_COUNTRY[self.country])

        self.ab_variant = rng.choice(AB_VARIANTS)

        self.state = "start"
        self.steps = 0
        self.last_product = None
        self.cart = []  # list of {"product_id", "name", "price", "quantity"}


class ClickstreamGenerator:
    """
    Maintains a pool of concurrent sessions and emits one event per call
    to next_event(). Sessions that reach "end" are replaced with a fresh
    one so the pool size (and therefore concurrency) stays constant.
    """

    def __init__(self, num_users: int = 5000, concurrent_sessions: int = 300,
                 num_products: int = 200, max_steps_per_session: int = 12,
                 seed: int | None = None):
        self.rng = random.Random(seed)
        self.catalog = ProductCatalog(num_products, seed=seed)
        self.max_steps = max_steps_per_session
        self.user_pool = [f"u-{uuid.uuid4().hex[:10]}" for _ in range(num_users)]
        self.sessions = [self._new_session() for _ in range(concurrent_sessions)]

    def _new_session(self) -> Session:
        user_id = self.rng.choice(self.user_pool)
        return Session(user_id, self.rng)

    def next_event(self) -> dict:
        idx = self.rng.randrange(len(self.sessions))
        session = self.sessions[idx]

        next_state = _weighted_choice(FUNNEL_TRANSITIONS[session.state])
        session.steps += 1
        if next_state == "end" or session.steps >= self.max_steps:
            event = self._build_event(session, "page_view" if session.state == "start"
                                       else session.state)
            self.sessions[idx] = self._new_session()
            return event

        session.state = next_state
        return self._build_event(session, next_state)

    def _page_url(self, event_type: str) -> str:
        if event_type == "search":
            return "https://shop.example.com/search"
        if event_type in ("product_view", "add_to_cart", "remove_from_cart", "purchase"):
            return "https://shop.example.com/product/" + uuid.uuid4().hex[:8]
        return "https://shop.example.com/"

    def _build_event(self, session: Session, event_type: str) -> dict:
        event = {
            "event_id": uuid.uuid4().hex,
            "event_time": int(time.time() * 1000),
            "event_type": event_type,
            "trace_id": uuid.uuid4().hex,
            "user_id": session.user_id,
            "session_id": session.session_id,
            "device": {
                "type": session.device,
                "os": session.os,
                "browser": session.browser,
                "user_agent": session.user_agent,
                "screen_resolution": session.screen_resolution,
            },
            "geo": {
                "country": session.country,
                "city": session.city,
                "region": session.region,
                "lat": session.lat,
                "lon": session.lon,
            },
            "marketing": {
                "referrer": session.referrer,
                "referrer_url": session.referrer_url,
                "utm_source": session.utm_source,
                "utm_campaign": session.utm_campaign,
            },
            "page_url": self._page_url(event_type),
            "ab_test_variant": session.ab_variant,
            "product_id": None,
            "product_name": None,
            "brand": None,
            "category": None,
            "price": None,
            "tags": None,
            "description": None,
            "quantity": None,
            "search_query": None,
            "amount": None,
            "cart": [dict(item) for item in session.cart],
            "cart_value": round(sum(i["price"] * i["quantity"] for i in session.cart), 2),
        }

        if event_type == "search":
            event["search_query"] = self.rng.choice(SEARCH_TERMS)

        elif event_type in ("product_view", "add_to_cart", "remove_from_cart", "purchase"):
            if event_type == "product_view" or session.last_product is None:
                product = self.catalog.sample()
                session.last_product = product
            else:
                product = session.last_product

            event["product_id"] = product["product_id"]
            event["product_name"] = product["name"]
            event["brand"] = product["brand"]
            event["category"] = product["category"]
            event["price"] = product["price"]
            event["tags"] = product["tags"]
            event["description"] = product["description"]

            if event_type == "add_to_cart":
                qty = self.rng.randint(1, 3)
                event["quantity"] = qty
                event["amount"] = round(product["price"] * qty, 2)
                session.cart.append({
                    "product_id": product["product_id"],
                    "name": product["name"],
                    "price": product["price"],
                    "quantity": qty,
                })
                event["cart"] = [dict(item) for item in session.cart]
                event["cart_value"] = round(sum(i["price"] * i["quantity"]
                                                 for i in session.cart), 2)

            elif event_type == "remove_from_cart" and session.cart:
                removed = session.cart.pop(self.rng.randrange(len(session.cart)))
                event["quantity"] = removed["quantity"]
                event["amount"] = round(removed["price"] * removed["quantity"], 2)
                event["cart"] = [dict(item) for item in session.cart]
                event["cart_value"] = round(sum(i["price"] * i["quantity"]
                                                 for i in session.cart), 2)

            elif event_type == "purchase":
                # Purchase the current cart contents (or the last-viewed
                # product as a single-item purchase if the cart is empty).
                items = session.cart if session.cart else [{
                    "product_id": product["product_id"],
                    "name": product["name"],
                    "price": product["price"],
                    "quantity": self.rng.randint(1, 3),
                }]
                event["cart"] = [dict(item) for item in items]
                event["amount"] = round(sum(i["price"] * i["quantity"] for i in items), 2)
                event["quantity"] = sum(i["quantity"] for i in items)
                session.cart = []
                event["cart_value"] = 0.0

        return event