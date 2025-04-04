"use client";

import React, { useState, useMemo } from "react";
import { Chart } from "react-google-charts";

type Metric = "hourly_wage" | "annual_wage";

type WageData = {
    occupation: string;
    hourly_wage: number;
    annual_wage: number;
};

type WageDashboardProps = {
    wages: WageData[];
};

export const WageDashboard: React.FC<WageDashboardProps> = ({ wages }) => {
    const [metric, setMetric] = useState<Metric>("hourly_wage");

    const maxValue = useMemo(() => {
        return Math.max(...wages.map((item) => item[metric]));
    }, [metric, wages]);

    const chartData = useMemo(() => {
        const axisTitle = metric === "hourly_wage" ? "Hourly Wage ($)" : "Annual Wage ($)";
        const validData = wages.filter((item) => item.occupation && item[metric] !== undefined);
        return [
            ["Occupation", axisTitle],
            ...validData.map((item) => [item.occupation, item[metric]]),
        ];
    }, [metric, wages]);

    const axisTitle = metric === "hourly_wage" ? "Hourly Wage ($)" : "Annual Wage ($)";
    const chartTitle = metric === "hourly_wage" ? "Hourly Wage by Occupation" : "Annual Wage by Occupation";

    return (
        <div className="p-4">
            <div className="mb-4">
                <label htmlFor="metric" className="font-medium mr-2">
                    Choose Metric:
                </label>
                <select
                    id="metric"
                    value={metric}
                    onChange={(e) => setMetric(e.target.value as Metric)}
                    className="border rounded px-2 py-1"
                >
                    <option value="hourly_wage">Hourly Wage</option>
                    <option value="annual_wage">Annual Wage</option>
                </select>
            </div>

            <Chart
                key={metric} // 👈 this is the trick
                chartType="BarChart"
                width="100%"
                height="500px"
                data={chartData}
                options={{
                    title: chartTitle,
                    chartArea: { width: "65%" },
                    hAxis: {
                        title: axisTitle,
                        minValue: 0,
                    },
                    vAxis: {
                        title: "Occupation",
                    },
                }}
                chartPackages={["corechart", "controls"]}
                controls={[
                    {
                        controlType: "NumberRangeFilter",
                        options: {
                            filterColumnIndex: 1,
                            ui: {
                                label: `Filter ${axisTitle}`,
                            },
                            minValue: 0,
                            maxValue: maxValue,
                        },
                    },
                ]}
                loader={<div>Loading Chart...</div>}
            />
        </div>
    );
};

export default WageDashboard;