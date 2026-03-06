
import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm

def hedge_ratio(a,b):
    y=np.log(a).reset_index(drop=True)
    x=np.log(b).reset_index(drop=True)
    X=sm.add_constant(x)
    model=sm.OLS(y,X).fit()
    return model.params.iloc[1]

def spread_z(a,b,beta):
    a = a.reset_index(drop=True)
    b = b.reset_index(drop=True)
    spread=np.log(a)-beta*np.log(b)
    z=(spread.iloc[-1]-spread.mean())/spread.std()
    return z

def half_life(spread):
    spread = spread.reset_index(drop=True)
    lag=spread.shift(1)
    delta=spread-lag
    df=pd.concat([delta,lag],axis=1).dropna()
    X=sm.add_constant(df.iloc[:,1])
    y=df.iloc[:,0]
    model=sm.OLS(y,X).fit()
    phi=model.params.iloc[1]

    if phi>=0:
        return 999

    return -np.log(2)/phi

def find_pairs(price_map):

    syms=list(price_map.keys())
    results=[]

    for i in range(len(syms)):
        for j in range(i+1,len(syms)):

            a=price_map[syms[i]].reset_index(drop=True)
            b=price_map[syms[j]].reset_index(drop=True)

            # Пропускаем пары с разной длиной истории
            if len(a) != len(b) or len(a) < 100:
                continue

            score,p,_=coint(a,b)

            if p>0.05:
                continue

            beta=hedge_ratio(a,b)

            spread=np.log(a)-beta*np.log(b)

            z=spread_z(a,b,beta)

            hl=half_life(spread)

            results.append((syms[i],syms[j],z,p,hl))

    results.sort(key=lambda x:abs(x[2]),reverse=True)

    return results
