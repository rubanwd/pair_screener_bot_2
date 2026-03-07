import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import coint
import statsmodels.api as sm

def hedge_ratio(a, b):
    # a, b: numpy arrays
    y = np.log(a)
    x = np.log(b)
    X = sm.add_constant(x)
    model = sm.OLS(y, X).fit()
    return model.params[1]

def spread_z(a, b, beta):
    # a, b: numpy arrays
    spread = np.log(a) - beta * np.log(b)
    z = (spread[-1] - spread.mean()) / spread.std()
    return z

def half_life(spread):
    # spread: numpy array
    lag = spread[:-1]
    delta = spread[1:] - lag
    X = sm.add_constant(lag)
    model = sm.OLS(delta, X).fit()
    phi = model.params[1]

    if phi >= 0:
        return 999

    return -np.log(2) / phi

def find_pairs(price_map, min_corr=0.7, p_max=0.05, hl_min=2.0, hl_max=72.0):
    
    # 1. Сначала объединяем все цены в один DataFrame
    # Это позволяет выровнять индексы и легко считать корреляцию
    df = pd.DataFrame(price_map)
    
    # Удаляем монеты, у которых слишком мало данных (< 100 свечей)
    df = df.dropna(axis=1, thresh=100)
    
    if df.empty or df.shape[1] < 2:
        return []

    # 2. Считаем матрицу корреляций (быстрый векторизованный фильтр)
    # Считаем логарифмические доходности
    returns = np.log(df).diff().dropna()
    corr_matrix = returns.corr()
    
    syms = list(df.columns)
    results = []

    # Предварительно извлекаем values (numpy массивы) для скорости
    prices_np = {sym: df[sym].values for sym in syms}

    for i in range(len(syms)):
        for j in range(i+1, len(syms)):
            sym_a = syms[i]
            sym_b = syms[j]
            
            # Быстрый фильтр по корреляции ДО тяжелых тестов
            corr = corr_matrix.loc[sym_a, sym_b]
            if corr < min_corr:
                continue

            # Для коинтеграции нужно убрать NaN, которые могут возникнуть из-за разной длины истории
            # Но так как мы выровняли их в DataFrame, можем просто взять маску валидных индексов
            mask = ~(np.isnan(prices_np[sym_a]) | np.isnan(prices_np[sym_b]))
            a = prices_np[sym_a][mask]
            b = prices_np[sym_b][mask]

            if len(a) < 100:
                continue

            # Тест Энгла-Грейнджера на коинтеграцию
            score, p, _ = coint(a, b)

            if p > p_max:
                continue

            # Считаем Hedge Ratio
            beta = hedge_ratio(a, b)
            
            # Строим спред
            spread = np.log(a) - beta * np.log(b)
            
            # Считаем Z-Score
            z = spread_z(a, b, beta)
            
            # Считаем период полураспада (Half-life)
            hl = half_life(spread)
            
            # Стратегический фильтр: отсекаем слишком быстрый или слишком долгий возврат к среднему
            if hl < hl_min or hl > hl_max:
                continue

            results.append((sym_a, sym_b, z, p, hl, corr))

    # Сортируем пары по абсолютному значению Z-Score (самые сильные отклонения)
    results.sort(key=lambda x: abs(x[2]), reverse=True)

    return results