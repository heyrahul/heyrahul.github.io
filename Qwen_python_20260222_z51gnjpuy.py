#!/usr/bin/env python3
"""
Generate 1,000 text-to-query training examples for PostgreSQL & MongoDB.
Output: JSONL format matching your schema.
"""
import json, random
from typing import List, Dict

NUM_EXAMPLES = 1000
OUTPUT_FILE = "training_data_1000.jsonl"
random.seed(42)

# === SCHEMAS ===
PG_SCHEMA = {
    "orders": "Table orders: id INT, customer_name VARCHAR, amount DECIMAL, created_at TIMESTAMP, status VARCHAR",
    "products": "Table products: id INT, name VARCHAR, category VARCHAR, price DECIMAL, stock INT, created_at TIMESTAMP",
    "users": "Table users: id INT, name VARCHAR, email VARCHAR, city VARCHAR, joined_at DATE, active BOOLEAN",
}
MO_SCHEMA = {
    "orders": "Collection orders: customer (string), amount (number), status (string), date (date), items (array)",
    "products": "Collection products: name (string), category (string), price (number), stock (number), tags (array)",
    "users": "Collection users: name (string), email (string), city (string), joined_at (date), preferences (object)",
}

# === POSTGRES QUERY GENERATORS ===
def pg_agg(t,m,tu): return f"SELECT DATE_TRUNC('{tu}',created_at)AS period,{m}(amount)AS v FROM {t} GROUP BY period ORDER BY period"
def pg_cum(t,m): return f"SELECT created_at::date AS d,{m}(amount)OVER(ORDER BY created_at::date)AS cum FROM {t} ORDER BY d"
def pg_grp_cnt(t,e,g): return f"SELECT {g},COUNT(*)AS {e}_c FROM {t} GROUP BY {g} ORDER BY {e}_c DESC"
def pg_grp_m(t,m,g): return f"SELECT {g},{m}(amount)AS total FROM {t} GROUP BY {g} ORDER BY total DESC"
def pg_filt(t,fc,fo,fv): return f"SELECT*FROM {t} WHERE {fc}{fo}{fv} ORDER BY created_at DESC LIMIT 100"
def pg_top(t,n,e,m): return f"SELECT {e},{m}(amount)AS s FROM {t} GROUP BY {e} ORDER BY s DESC LIMIT {n}"
def pg_dist(t,g): return f"SELECT {g},COUNT(*)AS c FROM {t} GROUP BY {g}"
def pg_scat(t,c1,c2): return f"SELECT {c1},{c2} FROM {t} WHERE {c1}IS NOT NULL AND {c2}IS NOT NULL ORDER BY {c1} DESC"

# === MONGO QUERY GENERATORS (using .format() to avoid f-string brace issues) ===
def mo_agg(c,m,tu):
    op="${}".format(m.lower())
    return 'db.{}.aggregate([{{"$group":{{"_id":{{"{}":"$date"}},"v":{{"{}":"$amount"}}}}}},{{"$sort":{{"_id":1}}}}])'.format(c,tu,op)
def mo_tot(c,m):
    op="${}".format(m.lower())
    return 'db.{}.aggregate([{{"$group":{{"_id":null,"total":{{"{}":"$amount"}}}}}}])'.format(c,op)
def mo_grp_cnt(c,f): return 'db.{}.aggregate([{{"$group":{{"_id":"${}","count":{{"$sum":1}}}}}},{{"$sort":{{"count":-1}}}}])'.format(c,f)
def mo_grp_m(c,m,f):
    op="${}".format(m.lower())
    return 'db.{}.aggregate([{{"$group":{{"_id":"${}","v":{{"{}":"$amount"}}}}}},{{"$sort":{{"v":-1}}}}])'.format(c,f,op)
def mo_filt(c,f,op,v): return 'db.{}.find({{"{}":{{"{}":{}}}}}).limit(100)'.format(c,f,op,v)
def mo_top(c,n,f,m):
    op="${}".format(m.lower())
    return 'db.{}.aggregate([{{"$group":{{"_id":"${}","s":{{"{}":"$amount"}}}}}},{{"$sort":{{"s":-1}}}},{{"$limit":{}}}])'.format(c,f,op,n)
def mo_low(c,f): return 'db.{}.find({{"{}":{{"$lt":10}}}}).limit(50)'.format(c,f)
def mo_scat(c,f1,f2): return 'db.{}.find({{}},{{"{}":1,"{}":1,"_id":0}}).limit(200)'.format(c,f1,f2)

# === TEMPLATES ===
PG_TPLS = [
    {"p":"Show {m} per {tu} for {t}","g":pg_agg,"c":"line","ms":["SUM","AVG","COUNT"],"tus":["day","week","month"],"ts":["orders","products"]},
    {"p":"Cumulative {m} over time for {t}?","g":pg_cum,"c":"area","ms":["SUM","COUNT"],"ts":["orders"]},
    {"p":"How many {e} per {g} in {t}?","g":pg_grp_cnt,"c":"bar","es":["orders","products"],"gs":["category","status","city"],"ts":["orders","products","users"]},
    {"p":"{m} by {g} for {t}","g":pg_grp_m,"c":"bar","ms":["SUM","AVG","MAX"],"gs":["customer_name","category"],"ts":["orders","products"]},
    {"p":"Find {t} where {fc} {fo} {fv}","g":pg_filt,"c":"bar","fs":[("stock","<",10),("amount",">",100)],"ts":["products","orders"]},
    {"p":"Top {n} {e} by {m} in {t}","g":pg_top,"c":"bar","ns":[3,5,10],"es":["customer_name","name"],"ms":["SUM","AVG"],"ts":["orders","products"]},
    {"p":"Distribution of {t} by {g}?","g":pg_dist,"c":"pie","gs":["category","status"],"ts":["products","orders"]},
    {"p":"Show {c1} vs {c2} for {t}","g":pg_scat,"c":"scatter","ps":[("price","stock"),("amount","created_at")],"ts":["products","orders"]},
]

MO_TPLS = [
    {"p":"Show {m} per {tu} for {c}","g":mo_agg,"c":"line","ms":["SUM","AVG"],"tus":["$month","$year"],"cs":["orders","events"]},
    {"p":"Total {m} for {c}?","g":mo_tot,"c":"bar","ms":["SUM"],"cs":["orders"]},
    {"p":"Count {c} by {f}","g":mo_grp_cnt,"c":"pie","fs":["status","category"],"cs":["orders","products"]},
    {"p":"{m} by {f} for {c}","g":mo_grp_m,"c":"bar","ms":["SUM","AVG"],"fs":["customer","category"],"cs":["orders"]},
    {"p":"Find {c} with {f} {op} {v}","g":mo_filt,"c":"bar","fs":[("stock","$lt",10),("amount","$gt",100)],"cs":["products","orders"]},
    {"p":"Top {n} {f} by {m} in {c}","g":mo_top,"c":"bar","ns":[3,5,10],"fs":["customer","category"],"ms":["SUM"],"cs":["orders"]},
    {"p":"Low {f} in {c}","g":mo_low,"c":"bar","fs":["stock","price"],"cs":["products"]},
    {"p":"{f1} vs {f2} for {c}","g":mo_scat,"c":"scatter","ps":[("price","stock"),("amount","date")],"cs":["products","orders"]},
]

# === GENERATION LOGIC ===
def gen_pg(tpl):
    t=random.choice(tpl["ts"]); schema=PG_SCHEMA[t]
    if "ms" in tpl: m=random.choice(tpl["ms"])
    if "tus" in tpl:
        tu=random.choice(tpl["tus"]); q=tpl["p"].format(m=m.lower(),tu=tu,t=t); qry=tpl["g"](t,m,tu)
    elif "gs" in tpl:
        g=random.choice([x for x in tpl["gs"] if x in schema.lower()] or tpl["gs"])
        e=random.choice(tpl.get("es",[g]))
        q=tpl["p"].format(e=e,g=g,t=t,m=m.lower()) if "ms" in tpl else tpl["p"].format(e=e,g=g,t=t)
        qry=tpl["g"](t,m,g) if "ms" in tpl else tpl["g"](t,e,g)
    elif "fs" in tpl and "ns" not in tpl:
        fc,fo,fv=random.choice(tpl["fs"]); q=tpl["p"].format(t=t,fc=fc,fo=fo,fv=fv); qry=tpl["g"](t,fc,fo,fv)
    elif "ns" in tpl:
        n=random.choice(tpl["ns"]); e=random.choice(tpl["es"]); m=random.choice(tpl["ms"])
        q=tpl["p"].format(n=n,e=e,m=m.lower(),t=t); qry=tpl["g"](t,n,e,m)
    elif "ps" in tpl:
        c1,c2=random.choice(tpl["ps"]); q=tpl["p"].format(t=t,c1=c1,c2=c2); qry=tpl["g"](t,c1,c2)
    else:
        g=random.choice(tpl["gs"]); q=tpl["p"].format(t=t,g=g); qry=tpl["g"](t,g)
    return {"text":q,"schema":schema,"dialect":"postgres","query":qry,"chart":tpl["c"]}

def gen_mo(tpl):
    c=random.choice(tpl["cs"]); schema=MO_SCHEMA[c]
    if "ms" in tpl: m=random.choice(tpl["ms"])
    if "tus" in tpl:
        tu=random.choice(tpl["tus"]); q=tpl["p"].format(m=m.lower(),tu=tu.replace("$",""),c=c); qry=tpl["g"](c,m,tu)
    elif "fs" in tpl:
        f=random.choice([x for x in tpl["fs"] if x in schema.lower()] or tpl["fs"])
        if "ns" in tpl:
            n=random.choice(tpl["ns"]); q=tpl["p"].format(n=n,f=f,m=m.lower(),c=c); qry=tpl["g"](c,n,f,m)
        elif isinstance(tpl["fs"][0],tuple):
            ff,op,v=random.choice(tpl["fs"]); q=tpl["p"].format(c=c,f=ff,op=op,v=v); qry=tpl["g"](c,ff,op,v)
        else:
            q=tpl["p"].format(c=c,f=f,m=m.lower()); qry=tpl["g"](c,m,f)
    elif "ps" in tpl:
        f1,f2=random.choice(tpl["ps"]); q=tpl["p"].format(c=c,f1=f1,f2=f2); qry=tpl["g"](c,f1,f2)
    else:
        f=random.choice(tpl["fs"]); q=tpl["p"].format(c=c,f=f); qry=tpl["g"](c,f)
    return {"text":q,"schema":schema,"dialect":"mongo","query":qry,"chart":tpl["c"]}

def generate(n=NUM_EXAMPLES):
    exs=[]; attempts=0
    while len(exs)<n and attempts<5000:
        attempts+=1
        try:
            if len(exs)<n//2: exs.append(gen_pg(random.choice(PG_TPLS)))
            else: exs.append(gen_mo(random.choice(MO_TPLS)))
        except: continue
    random.shuffle(exs)
    return exs[:n]

def save(exs,fn=OUTPUT_FILE):
    with open(fn,'w') as f:
        for e in exs: f.write(json.dumps(e)+'\n')
    print(f"✓ Saved {len(exs)} examples to {fn}")

if __name__=="__main__":
    print(f"🚀 Generating {NUM_EXAMPLES} examples...")
    examples=generate()
    save(examples)
    # Stats
    pg=sum(1 for e in examples if e['dialect']=='postgres')
    charts={c:sum(1 for e in examples if e['chart']==c) for c in ['bar','line','pie','scatter','area']}
    print(f"📊 PG:{pg} | Mongo:{len(examples)-pg} | Charts:{charts}")
    print(f"📝 First example:\n{json.dumps(examples[0],indent=2)}")