"use client"
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from "../ui/dropdown-menu";
import icon from "./icon.jpg";
import Image from "next/image";
import { ChevronDown } from "lucide-react"; // Import an arrow icon
import { useState } from "react"; // Import useState

export default function ProfileShow() {
    const [isOpen, setIsOpen] = useState(false); // State to manage dropdown visibility

    const toggleDropdown = () => {
        setIsOpen((prev) => !prev); // Toggle the dropdown state
    };

    return (
        <>
            <DropdownMenu>
                <DropdownMenuTrigger>
                    <div
                        className="flex items-center cursor-pointer space-x-2"
                        onClick={toggleDropdown} // Attach the toggle function
                    >
                        <div className="relative">
                            <Image
                                src={icon}
                                alt="User Avatar"
                                className="rounded-full w-10 h-10 object-cover border-2 border-white shadow-sm"
                            />
                            <span className="absolute bottom-0 right-0 w-3 h-3 bg-green-500 rounded-full border-2 border-gray-100" />
                        </div>
                        {/* Add an arrow icon */}
                        <ChevronDown className="text-gray-500" />
                    </div>
                </DropdownMenuTrigger>
                <div >
                {isOpen && ( 
                    <div className="font-bold text-gray-700 w-full px-4 py-2 hover:bg-gray-100">
                    <DropdownMenuContent>
                        <DropdownMenuLabel>My Account</DropdownMenuLabel>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem>Profile</DropdownMenuItem>
                        <DropdownMenuItem>Settings</DropdownMenuItem>
                        <DropdownMenuSeparator />
                        <DropdownMenuItem>Logout</DropdownMenuItem>
                    </DropdownMenuContent>
                </div>
                )}
                </div>
            </DropdownMenu>
        </>
    );
}
