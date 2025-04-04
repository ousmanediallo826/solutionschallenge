"use client"
import { useState } from "react";
import { Convertions } from "../components/unify";
import { AppColumn } from "../components/chart/google-graph/ColumnChart";
import { WageDashboard } from "../components/chart/google-graph/filterChart";
import wages from "../../app-data/wages";

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
      <div className="text-center mb-6">
        All employeers from public, private and government sectors.
      </div>
      <div className="flex space-x-4 mb-4">
        <button
          className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
          onClick={() => handleClick('a')}
          aria-label="Show AppColumn"
        >
          A
        </button>
        <button
          className="bg-green-500 hover:bg-green-700 text-white font-bold py-2 px-4 rounded"
          onClick={() => handleClick('b')}
          aria-label="Show WageDashboard"
        >
          B
        </button>
        <button
          className="bg-indigo-500 hover:bg-indigo-700 text-white font-bold py-2 px-4 rounded"
          onClick={() => handleClick('c')}
          aria-label="Show Convertions"
        >
          C
        </button>
      </div>
      <div className="mt-4 p-4 bg-white rounded-md border border-gray-200">
        {displayedComponent ? (
          displayedComponent
        ) : (
          <p className="text-gray-500 italic">Click A, B, or C</p>
        )}
      </div>
    </div>
  );
}
