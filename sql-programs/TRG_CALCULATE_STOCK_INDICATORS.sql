CREATE OR ALTER TRIGGER TRG_CALCULATE_STOCK_INDICATORS
ON [dbo].[stock_daily_data]
AFTER INSERT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Declare variables for the inserted ticker and date
    DECLARE @ticker NVARCHAR(50), @date DATE;
    
    -- Get the ticker and date from the inserted row
    SELECT @ticker = ticker, @date = [Date] FROM inserted;
    
    -- Update the technical indicators for the newly inserted row
    UPDATE sdd
    SET 
        -- Moving Averages
        MA20 = ma20_calc.MA20,
        MA50 = ma50_calc.MA50,
        MA200 = ma200_calc.MA200,
        
        -- RSI Calculation
        RSI = rsi_calc.RSI,
        
        -- MACD Indicators
        MACD = macd_calc.MACD,
        MACD_Signal = macd_calc.MACD_Signal,
        MACD_Hist = macd_calc.MACD_Hist,
        
        -- Daily Return
        Daily_Return = CASE 
                          WHEN prev_day.Close IS NOT NULL 
                          THEN ((sdd.Close - prev_day.Close) / prev_day.Close) * 100
                          ELSE NULL
                       END,
        
        -- Volatility (20-day standard deviation of returns)
        Volatility_20d = vol_calc.Volatility,
        
        -- Volume Moving Average and Ratio
        Volume_MA20 = vol_ma_calc.Volume_MA20,
        Volume_Ratio = CASE 
                          WHEN vol_ma_calc.Volume_MA20 > 0 
                          THEN sdd.Volume / vol_ma_calc.Volume_MA20
                          ELSE NULL
                       END
    FROM 
        [dbo].[stock_daily_data] sdd
    INNER JOIN 
        inserted i ON sdd.ticker = i.ticker AND sdd.[Date] = i.[Date]
    
    -- Previous day's close for calculating daily return
    LEFT JOIN (
        SELECT ticker, [Date], Close
        FROM [dbo].[stock_daily_data]
        WHERE ticker = @ticker AND [Date] < @date
        ORDER BY [Date] DESC
        OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY
    ) prev_day ON 1=1
    
    -- Calculate MA20 (20-day moving average of closing prices)
    LEFT JOIN (
        SELECT 
            AVG(Close) AS MA20
        FROM (
            SELECT TOP 20 Close
            FROM [dbo].[stock_daily_data]
            WHERE ticker = @ticker AND [Date] <= @date
            ORDER BY [Date] DESC
        ) AS last_20_days
    ) ma20_calc ON 1=1
    
    -- Calculate MA50 (50-day moving average of closing prices)
    LEFT JOIN (
        SELECT 
            AVG(Close) AS MA50
        FROM (
            SELECT TOP 50 Close
            FROM [dbo].[stock_daily_data]
            WHERE ticker = @ticker AND [Date] <= @date
            ORDER BY [Date] DESC
        ) AS last_50_days
    ) ma50_calc ON 1=1
    
    -- Calculate MA200 (200-day moving average of closing prices)
    LEFT JOIN (
        SELECT 
            AVG(Close) AS MA200
        FROM (
            SELECT TOP 200 Close
            FROM [dbo].[stock_daily_data]
            WHERE ticker = @ticker AND [Date] <= @date
            ORDER BY [Date] DESC
        ) AS last_200_days
    ) ma200_calc ON 1=1
    
    -- Calculate RSI (14-day Relative Strength Index)
    LEFT JOIN (
        SELECT 
            100 - (100 / (1 + (SUM(CASE WHEN price_diff > 0 THEN price_diff ELSE 0 END) / 
                          CASE WHEN SUM(CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END) = 0 
                               THEN 1 
                               ELSE SUM(CASE WHEN price_diff < 0 THEN ABS(price_diff) ELSE 0 END) 
                          END))) AS RSI
        FROM (
            SELECT 
                Close - LAG(Close, 1, Close) OVER (ORDER BY [Date]) AS price_diff
            FROM [dbo].[stock_daily_data]
            WHERE ticker = @ticker AND [Date] <= @date
            ORDER BY [Date] DESC
            OFFSET 0 ROWS FETCH NEXT 15 ROWS ONLY
        ) AS rsi_data
    ) rsi_calc ON 1=1
    
    -- Calculate MACD indicators
    LEFT JOIN (
        SELECT 
            ema_12 - ema_26 AS MACD,
            signal_line AS MACD_Signal,
            (ema_12 - ema_26) - signal_line AS MACD_Hist
        FROM (
            SELECT 
                -- 12-day EMA
                (SELECT 
                    SUM(Close * (1 - 2.0/(12+1))^(ROW_NUMBER() OVER (ORDER BY [Date] DESC) - 1)) / 
                    SUM((1 - 2.0/(12+1))^(ROW_NUMBER() OVER (ORDER BY [Date] DESC) - 1))
                FROM [dbo].[stock_daily_data]
                WHERE ticker = @ticker AND [Date] <= @date
                ORDER BY [Date] DESC
                OFFSET 0 ROWS FETCH NEXT 26 ROWS ONLY) AS ema_12,
                
                -- 26-day EMA
                (SELECT 
                    SUM(Close * (1 - 2.0/(26+1))^(ROW_NUMBER() OVER (ORDER BY [Date] DESC) - 1)) / 
                    SUM((1 - 2.0/(26+1))^(ROW_NUMBER() OVER (ORDER BY [Date] DESC) - 1))
                FROM [dbo].[stock_daily_data]
                WHERE ticker = @ticker AND [Date] <= @date
                ORDER BY [Date] DESC
                OFFSET 0 ROWS FETCH NEXT 26 ROWS ONLY) AS ema_26,
                
                -- 9-day EMA of MACD (Signal Line)
                (SELECT 
                    SUM((ema12.val - ema26.val) * (1 - 2.0/(9+1))^(ROW_NUMBER() OVER (ORDER BY ema12.[Date] DESC) - 1)) / 
                    SUM((1 - 2.0/(9+1))^(ROW_NUMBER() OVER (ORDER BY ema12.[Date] DESC) - 1))
                FROM (
                    SELECT [Date], 
                        SUM(Close * (1 - 2.0/(12+1))^(ROW_NUMBER() OVER (ORDER BY [Date] DESC) - 1)) / 
                        SUM((1 - 2.0/(12+1))^(ROW_NUMBER() OVER (ORDER BY [Date] DESC) - 1)) AS val
                    FROM [dbo].[stock_daily_data]
                    WHERE ticker = @ticker AND [Date] <= @date
                    GROUP BY [Date]
                    ORDER BY [Date] DESC
                    OFFSET 0 ROWS FETCH NEXT 35 ROWS ONLY
                ) ema12
                JOIN (
                    SELECT [Date], 
                        SUM(Close * (1 - 2.0/(26+1))^(ROW_NUMBER() OVER (ORDER BY [Date] DESC) - 1)) / 
                        SUM((1 - 2.0/(26+1))^(ROW_NUMBER() OVER (ORDER BY [Date] DESC) - 1)) AS val
                    FROM [dbo].[stock_daily_data]
                    WHERE ticker = @ticker AND [Date] <= @date
                    GROUP BY [Date]
                    ORDER BY [Date] DESC
                    OFFSET 0 ROWS FETCH NEXT 35 ROWS ONLY
                ) ema26 ON ema12.[Date] = ema26.[Date]
                ORDER BY ema12.[Date] DESC
                OFFSET 0 ROWS FETCH NEXT 9 ROWS ONLY) AS signal_line
        ) AS macd_data
    ) macd_calc ON 1=1
    
    -- Calculate 20-day volatility (standard deviation of returns)
    LEFT JOIN (
        SELECT 
            STDEV(daily_return) * SQRT(252) AS Volatility
        FROM (
            SELECT 
                ((Close - LAG(Close, 1) OVER (ORDER BY [Date])) / LAG(Close, 1) OVER (ORDER BY [Date])) AS daily_return
            FROM [dbo].[stock_daily_data]
            WHERE ticker = @ticker AND [Date] <= @date
            ORDER BY [Date] DESC
            OFFSET 0 ROWS FETCH NEXT 21 ROWS ONLY
        ) AS return_data
    ) vol_calc ON 1=1
    
    -- Calculate 20-day volume moving average
    LEFT JOIN (
        SELECT 
            AVG(CAST(Volume AS DECIMAL(20,2))) AS Volume_MA20
        FROM (
            SELECT TOP 20 Volume
            FROM [dbo].[stock_daily_data]
            WHERE ticker = @ticker AND [Date] <= @date
            ORDER BY [Date] DESC
        ) AS last_20_days_vol
    ) vol_ma_calc ON 1=1;
END;
