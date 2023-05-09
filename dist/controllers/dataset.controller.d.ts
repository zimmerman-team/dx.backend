export declare class FileController {
    processFile(fileName: string): Promise<{
        error: string;
        fileName?: undefined;
    } | {
        fileName: string;
        error?: undefined;
    }>;
    deleteDataset(fileName: string): Promise<{
        error: string;
        fileName?: undefined;
    } | {
        fileName: string;
        error?: undefined;
    }>;
}
