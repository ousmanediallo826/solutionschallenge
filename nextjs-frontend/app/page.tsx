import Image from "next/image";
import DashboardHome from "./src/dashboard/page";

import Header from '../app/src/components/header/base-header';

export default function Home() {

  return (
    <>
      <div className="min-h-screen flex flex-col bg-gray-100 text-gray-900 antialiased">
        {/* Header */}
        <header className="bg-white shadow-md py-4 px-6">
          <Header />
        </header>
        <div>
          <DashboardHome />
        </div>
      </div>
    </>
  );
}
