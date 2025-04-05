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
import { Button } from '..//ui/button';
import { Input } from '..//ui/input';
import ProfileShow from './profile-header'
export default function Header() {

    return (
        <>
            <div className="container mx-auto flex justify-between items-center">
                <div className="flex items-center">
                    <LayoutDashboard className="text-indigo-500 mr-2 text-xl" />
                    <h1 className="text-xl font-semibold text-gray-800">AI-Driver</h1>
                </div>
                {/*<div className="flex items-center space-x-4">
              <div className="relative">
                <Input
                  type="text"
                  placeholder="Search..."
                  className="bg-gray-100 rounded-md py-2 px-4 focus:outline-none focus:ring-2 focus:ring-indigo-500"
                />
                <Search className="absolute right-3 top-3 text-gray-500" />
              </div>
                <Bell className="text-gray-500" />*/}
                <ProfileShow />
            </div>
        </>
    );
}
