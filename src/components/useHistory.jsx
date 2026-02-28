import { useState, useCallback } from "react";

export const useHistory = () => {
  const [history, setHistory] = useState([]);
  const [historyIndex, setHistoryIndex] = useState(-1);

  const push = useCallback((action) => {
    setHistory(prev => {
      const newHistory = prev.slice(0, historyIndex + 1);
      newHistory.push(action);
      return newHistory;
    });
    setHistoryIndex(prev => prev + 1);
  }, []);

  const undo = useCallback(() => {
    setHistoryIndex(prev => {
      if (prev > 0) {
        return prev - 1;
      }
      return prev;
    });
  }, []);

  const redo = useCallback(() => {
    setHistoryIndex(prev => {
      setHistory(h => {
        if (prev < h.length - 1) {
          return h;
        }
        return h;
      });
      return prev < history.length - 1 ? prev + 1 : prev;
    });
  }, [history.length]);

  const canUndo = historyIndex > 0;
  const canRedo = historyIndex < history.length - 1;

  return { push, undo, redo, canUndo, canRedo, history, historyIndex };
};