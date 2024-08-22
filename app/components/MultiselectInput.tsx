"use client";

import { forwardRef } from 'react';
import { Combobox } from '@headlessui/react';
import { Icon } from './Icon';
import { useFormContext } from './FormContext';

export interface MultiselectInputProps {
  className?: string;
  name?: string;
  items: { label: string; value: string }[];
  value?: string[];
  defaultValue?: string[];
  placeholder?: string;
  onChange?(value: string[]): void;
  onBlur?(): void;
}

const MultiselectInput = forwardRef<HTMLInputElement, MultiselectInputProps>(
  (
    {
      className = '',
      value,
      onChange,
      onBlur,
      items,
      name,
      defaultValue,
      placeholder,
    },
    ref
  ) => {
    const formContext = useFormContext();
    const hasErrors = (formContext?.errors?.[name ?? '']?.length ?? 0) > 0

    return (
      <div className={`${className} relative`}>
        <Combobox
          value={value}
          onChange={onChange}
          multiple
          name={name}
          defaultValue={defaultValue}
        >
          <div
            className={`
            border rounded shadow-inner flex
            has-[:focus-visible]:outline outline-2
            bg-white dark:bg-gray-800
            ${
              hasErrors
                ? 'border-red-700 shadow-red-100 outline-red-700'
                : 'border-gray-400 dark:border-gray-500 outline-green-300'
            }
          `}
          >
            <Combobox.Input
              className="w-full py-2 px-3 h-9 flex-grow focus:outline-none bg-transparent rounded"
              readOnly
              ref={ref}
              onBlur={onBlur}
              displayValue={(value: string[]) =>
                value
                  .map((v) => items.find((i) => i.value === v)?.label ?? '')
                  .join(', ')
              }
              placeholder={placeholder}
            />
            <Combobox.Button className="w-8">
              {({ open }) => <Icon icon={open ? 'caret-up' : 'caret-down'} />}
            </Combobox.Button>
          </div>
          <Combobox.Options
            className="
              absolute z-20 mt-1 max-h-80 w-full overflow-auto rounded border border-gray-400 bg-white shadow
              dark:bg-gray-700 dark:border-gray-600
            "
          >
            {items.map((item) => (
              <Combobox.Option
                className="px-3 py-2 ui-active:bg-green-200 dark:ui-active:green-400 dark:ui-active:text-gray-800"
                key={item.value}
                value={item.value}
              >
                {({ selected }) => (
                  <>
                    <span className="inline-block w-6">
                      {selected && <Icon icon="check" />}
                    </span>
                    {item.label}
                  </>
                )}
              </Combobox.Option>
            ))}
          </Combobox.Options>
        </Combobox>
      </div>
    );
  }
);
MultiselectInput.displayName= "MultiselectInput"
export default MultiselectInput;

