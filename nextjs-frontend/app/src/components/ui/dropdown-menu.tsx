export function DropdownMenu({ children }: { children: React.ReactNode }) {
    return <div className="relative">{children}</div>;
  }
  
  export function DropdownMenuContent({ children }: { children: React.ReactNode }) {
    return <div className="absolute bg-white shadow-md rounded">{children}</div>;
  }
  
  export function DropdownMenuItem({ children }: { children: React.ReactNode }) {
    return <div className="px-4 py-2 hover:bg-gray-100">{children}</div>;
  }
  
  export function DropdownMenuLabel({ children }: { children: React.ReactNode }) {
    return <div className="px-4 py-2 font-bold">{children}</div>;
  }
  
  export function DropdownMenuSeparator() {
    return <hr className="border-t" />;
  }
  
  export function DropdownMenuTrigger({ children }: { children: React.ReactNode }) {
    return <div className="cursor-pointer">{children}</div>;
  }