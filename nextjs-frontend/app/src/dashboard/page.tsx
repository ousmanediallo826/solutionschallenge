"use client";
import { useState } from "react";
import { Convertions } from "../components/unify";
import { AppColumn } from "../components/chart/google-graph/ColumnChart";
import { WageDashboard } from "../components/chart/google-graph/filterChart";
import wages from "../../app-data/wages";
import Info from "./informationCharts";

export default function Home() {
  const [displayedComponent, setDisplayedComponent] = useState<React.ReactNode>(null);

  const handleClick = (button: 'a' | 'b' | 'c') => {
    switch (button) {
      case 'a':
        setDisplayedComponent(<AppColumn />);
        break;
      case 'b':
        setDisplayedComponent(<WageDashboard wages={wages} />);
        break;
      case 'c':
        setDisplayedComponent(<Convertions />);
        break;
      default:
        console.warn(`Unexpected button value: ${button}`);
        setDisplayedComponent(null);
    }
  };

  return (
    <div className="min-h-screen flex flex-col items-center justify-center bg-gray-100 p-6">
      <div className="text-2xl font-bold text-center mb-6">
        All employeers from public, private and government sectors.
      </div>
      {/* Add spacing between the buttons and <Info /> */}
      <div className="mb-6">
        {/* Center the buttons */}
        <div className="flex justify-center space-x-4 text-center mb-4">
          <button
            className="bg-blue-500 text-center hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
            onClick={() => handleClick('a')}
            aria-label="Show AppColumn"
          >
            Bar chart
          </button>
          <button
            className="bg-green-500 text-center hover:bg-green-700 text-white font-bold py-2 px-4 rounded"
            onClick={() => handleClick('b')}
            aria-label="Show WageDashboard"
          >
            Vertical Bar Chart (options)
          </button>
          <button
            className="bg-indigo-500 text-center hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded"
            onClick={() => handleClick('c')}
            aria-label="Show Convertions"
          >
            Circles chart
          </button>
        </div>
        <div className="mt-4 p-4 bg-white rounded-md border border-gray-200">
          {displayedComponent ? (
            displayedComponent
          ) : (
            <p className="text-gray-500 italic">Click A, B, or C</p>
          )}
        </div>
        {/* Add margin-top to create space between the buttons and <Info /> */}
        <br></br>
        <Info />
      </div>
    </div>
  );
}
