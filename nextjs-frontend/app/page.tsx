import Image from "next/image";
import DashboardHome from "./src/dashboard/page";
import { AppColumn } from "./src/google-graph/ColumnChart";
import { WageDashboard } from "./src/google-graph/filterChart";
import wages from "./app-data/wages";

import {
  User,
  ShoppingCart,
  PercentCircle,
  Search,
  Bell,
  ChevronDown,
  Table,
  Users,
  LayoutDashboard
} from 'lucide-react';
import { Button } from '../app/src/components/ui/button';
import { Input } from '../app/src/components/ui/input';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "../app/src/components/ui/dropdown-menu";

export default function Home() {

  return (
    <>
      <div className="min-h-screen flex flex-col bg-gray-100 text-gray-900 antialiased">
        {/* Header */}
        <header className="bg-white shadow-md py-4 px-6">
          <div className="container mx-auto flex justify-between items-center">
            <div className="flex items-center">
              <LayoutDashboard className="text-indigo-500 mr-2 text-xl" />
              <h1 className="text-xl font-semibold text-gray-800">Dashboard Overview</h1>
            </div>
            <div className="flex items-center space-x-4">
              <div className="relative">
                <Input
                  type="text"
                  placeholder="Search..."
                  className="bg-gray-100 rounded-md py-2 px-4 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                />
                <Search className="absolute right-3 top-3 text-gray-500" />
              </div>
              <Bell className="text-gray-500" />
              <DropdownMenu>
                <DropdownMenuTrigger>
                  <div className="relative cursor-pointer">
                    <img
                      src="https://via.placeholder.com/40"
                      alt="User Avatar"
                      className="rounded-full w-10 h-10 object-cover border-2 border-white shadow-sm"
                    />
                    <span className="absolute bottom-0 right-0 w-3 h-3 bg-green-500 rounded-full border-2 border-gray-100" />
                  </div>
                </DropdownMenuTrigger>
                <DropdownMenuContent>
                  <DropdownMenuLabel>My Account</DropdownMenuLabel>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem>Profile</DropdownMenuItem>
                  <DropdownMenuItem>Settings</DropdownMenuItem>
                  <DropdownMenuSeparator />
                  <DropdownMenuItem>Logout</DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </div>
          </div>
        </header>
        <div>
          All employeers from public, private and government sectors.
        </div>
        <div>
          using google react
          <AppColumn />
          <WageDashboard wages={wages} />
        </div>
        <div>
          <p>Using the sample vector</p>
          <DashboardHome />
        </div>
      </div>
    </>
  );
}
